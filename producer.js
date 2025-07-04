require('dotenv').config();
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL;

async function publishPayloads(payloads) {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    const channel = await conn.createChannel();

    await channel.assertQueue('outbound.calls.queue', { durable: true });

    //  Clear old messages before sending new ones
    // await channel.purgeQueue('outbound.calls.queue');
    // console.log(' Queue cleared before publishing new payloads');

    payloads.forEach(payload => {
      if (!payload.customer.number.startsWith('+91')) {
        payload.customer.number = '+91' + payload.customer.number;
      }

      channel.sendToQueue(
        'outbound.calls.queue',
        Buffer.from(JSON.stringify(payload)),
        { persistent: true }
      );

      console.log('Published:', payload.customer.number);
    });

    await channel.close();
    await conn.close();
    console.log(' All payloads published to queue');
  } catch (err) {
    console.error(' Producer error:', err.message);
  }
}

// Example usage
const payloads = [
  {
    assistantId: '4c8dcf92-48e9-44c3-8a3f-69676799fc9f',
    phoneNumberId: '59090934-7b8b-45d6-9565-88a9cb6646fb',
    customer: { number: '8102856535' }
  },
  {
    assistantId: '4c8dcf92-48e9-44c3-8a3f-69676799fc9f',
    phoneNumberId: '59090934-7b8b-45d6-9565-88a9cb6646fb',
    customer: { number: '9582118848' }
  },
 

];

publishPayloads(payloads);
