'use strict';

const dateFns = require('date-fns');
const { sendSQSMessage } = require('../utils');

module.exports.handler = async function () {
  const latest = dateFns.startOfDay(new Date()).getTime() / 1000;
  console.log(`sending SQS message with latest: ${latest}`);
  const params = {
    MessageBody: JSON.stringify({ latest }),
    QueueUrl: process.env.AWS_SQS_URL
  };
  await sendSQSMessage(params);
};
