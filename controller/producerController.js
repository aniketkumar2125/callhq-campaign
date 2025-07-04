// controllers/producerController.js
require('dotenv').config();
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = 'outbound.calls.queue';

exports.publishCampaign = async (req, res) => {
  const payloads = req.body;

  if (!Array.isArray(payloads)) {
    return res.status(400).json({ error: 'Payload must be an array of JSON objects' });
  }

  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    const channel = await conn.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });

    payloads.forEach(payload => {
      if (!payload.customer?.number) return;

      if (!payload.customer.number.startsWith('+91')) {
        payload.customer.number = '+91' + payload.customer.number;
      }

      channel.sendToQueue(
        QUEUE_NAME,
        Buffer.from(JSON.stringify(payload)),
        { persistent: true }
      );
      console.log('📤 Published:', payload.customer.number);
    });

    await channel.close();
    await conn.close();

    res.status(200).json({ message: '✅ Campaign published to queue' });
  } catch (err) {
    console.error('❌ Publish error:', err.message);
    res.status(500).json({ error: 'Failed to publish messages' });
  }
};
