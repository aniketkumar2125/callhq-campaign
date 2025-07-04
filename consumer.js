require('dotenv').config();
const express = require('express');
const amqp = require('amqplib');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 4000;

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const BEARER_TOKEN = process.env.BEARER_TOKEN;
const ASSISTANT_ID = process.env.ASSISTANT_ID;
const CALL_LIST_API = `https://backend-callhq-385086326930.asia-south1.run.app/api/calls?assistantId=${ASSISTANT_ID}`;
const PLACE_CALL_API = 'https://backend-callhq-385086326930.asia-south1.run.app/api/calls/outbound';

const MAX_CONCURRENT_CALLS = 2;
const QUEUE_NAME = 'outbound.calls.queue';

let isRunning = false; // Prevent duplicate workers

async function getActiveCallCount() {
  try {
    const res = await axios.get(CALL_LIST_API, {
      headers: { Authorization: `Bearer ${BEARER_TOKEN}` }
    });

    const active = res.data.calls.filter(call =>
      ['queued', 'in-progress'].includes(call.status)
    );
    return active.length;
  } catch (err) {
    console.error('❌ Error fetching call list:', err.message);
    return MAX_CONCURRENT_CALLS;
  }
}

async function startCampaignWorker() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    const channel = await conn.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });
    console.log('📞 Campaign worker started...');

    setInterval(async () => {
      const activeCalls = await getActiveCallCount();
      const availableSlots = MAX_CONCURRENT_CALLS - activeCalls;

      if (availableSlots <= 0) {
        console.log(`⏳ ${activeCalls} active calls — waiting...`);
        return;
      }

      for (let i = 0; i < availableSlots; i++) {
        const msg = await channel.get(QUEUE_NAME, { noAck: false });
        if (!msg) {
          console.log('📭 Queue is empty');
          break;
        }

        const payload = JSON.parse(msg.content.toString());
        console.log('📦 Processing:', payload.customer.number);

        try {
          const response = await axios.post(PLACE_CALL_API, payload, {
            headers: {
              Authorization: `Bearer ${BEARER_TOKEN}`,
              'Content-Type': 'application/json'
            }
          });

          if (response.status === 200 || response.status === 201) {
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
    }, 5000);
  } catch (err) {
    console.error('❌ Worker setup failed:', err.message);
  }
}

// 🚀 API endpoint to start campaign
app.post('/start-campaign', async (req, res) => {
  if (isRunning) {
    return res.status(200).json({ message: 'Campaign already running' });
  }

  isRunning = true;
  startCampaignWorker();
  res.status(200).json({ message: '📞 Campaign worker started' });
});

app.listen(PORT, () => {
  console.log(`🚀 Campaign API running on http://localhost:${PORT}`);
});
