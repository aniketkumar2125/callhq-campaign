require('dotenv').config();
const express = require('express');
const app = express();
const campaignRoutes = require('./router/consumerRoute');
const producerRoute = require('./router/producerRoute')

const PORT = process.env.PORT || 4000;

app.use(express.json());
app.use('/api/test', (req, res) =>{
  res.status(200).json({ message: 'Campaign API is running' });
})
app.use('/api', campaignRoutes);
app.use('/api', producerRoute)

app.listen(PORT, () => {
  console.log(`Campaign API running at http://localhost:${PORT}`);
});
