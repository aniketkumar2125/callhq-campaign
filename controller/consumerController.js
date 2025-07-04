// controllers/campaignController.js
const dotenv = require('dotenv');
const amqp = require('amqplib');
const axios = require('axios');

dotenv.config();

const BASE_API_URL = process.env.JARVIS_ASSISTANTS_ENDPOINT || 'https://api.vapi.ai';

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const BEARER_TOKEN = process.env.BEARER_TOKEN;
const ASSISTANT_ID = process.env.ASSISTANT_ID;

const PLACE_CALL_API = `${BASE_API_URL}/call`;
const QUEUE_NAME = 'outbound.calls.queue';
const MAX_CONCURRENT_CALLS = 2;

const CALLS_LIST_ENDPOINT = `${BASE_API_URL}/call`;

let isRunning = false;

async function getActiveCallCount() {
    const apiKey = process.env.JARVIS_API_KEY;

    try {
        const response = await axios.get(CALLS_LIST_ENDPOINT, {
            headers: { Authorization: `Bearer ${apiKey}` },
            params: { limit: 10 },
        });

        const active = response.data.filter(call =>
            ['queued', 'in-progress'].includes(call.status)
        );
        return active.length;
    } catch (err) {
        console.error('❌ Error fetching call list:', err.message);
        return MAX_CONCURRENT_CALLS;
    }
}

async function startCampaignWorker() {
    let conn, channel;
    let interval;

    try {
        conn = await amqp.connect(RABBITMQ_URL);
        channel = await conn.createChannel();
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        console.log('📞 Campaign worker started...');

        interval = setInterval(async () => {
            const activeCalls = await getActiveCallCount();
            const availableSlots = MAX_CONCURRENT_CALLS - activeCalls;

            if (availableSlots <= 0) {
                console.log(`⏳ ${activeCalls} active calls — waiting...`);
                return;
            }

            let queueEmpty = true;


            for (let i = 0; i < availableSlots; i++) {
                const msg = await channel.get(QUEUE_NAME, { noAck: false });
                if (!msg) break;

                queueEmpty = false;
                const payload = JSON.parse(msg.content.toString());

                console.log('📦 Processing:', payload.customer?.number || 'Unknown number');

                try {
                    // here in this payload 
                    // {
                    //    {
                    // "assistantId": "4c8dcf92-48e9-44c3-8a3f-69676799fc9f",
                    // "phoneNumberId": "59090934-7b8b-45d6-9565-88a9cb6646fb",
                    // "customer": { "number": "8102856535" }
                    //   }
                    //   }
                    // we have to map this assistant id with the external assistant id
                    const response = await axios.post(PLACE_CALL_API, payload, {
                        headers: {
                            Authorization: `Bearer ${process.env.JARVIS_API_KEY}`,
                            'Content-Type': 'application/json',
                        },
                    });

                    if ([200, 201].includes(response.status)) {
                        console.log('✅ Call placed:', payload.customer.number);
                        channel.ack(msg);
                    } else {
                        console.warn(`⚠️ API error: ${response.status} ${response.statusText}`);
                    }
                } catch (apiErr) {
                    console.error('❌ Failed to place call:', payload.customer.number);
                    console.error(apiErr.response?.data || apiErr.message);
                }
            }

            if (queueEmpty) {
                console.log('🎯 Queue is empty. Stopping worker...');
                clearInterval(interval);
                await channel.close();
                await conn.close();
                isRunning = false;
            }
        }, 5000);
    } catch (err) {
        console.error('❌ Worker setup failed:', err.message);
        if (interval) clearInterval(interval);
        if (channel) await channel.close().catch(() => { });
        if (conn) await conn.close().catch(() => { });
        isRunning = false;
    }
}

exports.startCampaign = async (req, res) => {
    const { assistantId } = req.body;
    if (isRunning) {
        return res.status(200).json({ message: 'Campaign already running' });
    }

    isRunning = true;
    startCampaignWorker();
    return res.status(200).json({ message: '📞 Campaign worker started' });
};
