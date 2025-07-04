// routes/campaignRoutes.js
const express = require('express');
const router = express.Router();
const { startCampaign } = require('../controller/consumerController');

router.post('/start-campaign', startCampaign);

module.exports = router;
