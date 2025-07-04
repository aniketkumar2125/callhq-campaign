// routes/producerRoutes.js
const express = require('express');
const router = express.Router();
const { publishCampaign } = require('../controller/producerController');

router.post('/publish-campaign', publishCampaign);

module.exports = router;
