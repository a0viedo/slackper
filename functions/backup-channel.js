'use strict';

const streamifier = require('streamifier');
const eos = require('end-of-stream');
const dateFns = require('date-fns');
const request = require('request-promise');
const fs = require('fs');
const { formatToTimeZone } = require('date-fns-timezone');

const { getMessagesFromDatabase, saveToDatabase, sendSQSMessage, saveFileToGitHub } = require('../utils/index.js');
const usersMap = {};

module.exports.handler = async function (event, context) {
  const repoDirectory = `/tmp/${context.awsRequestId}`;
  const gitParams = `--work-tree ${repoDirectory} --git-dir ${repoDirectory}/.git`;
  console.dir(event, {depth: 7});

  try {
    const day = dateFns.subDays(new Date(), 1);
    const START_OF_YESTERDAY = process.env.START_TIME ? Number(process.env.START_TIME) : dateFns.startOfDay(day);
    const triggerMessage = JSON.parse(event.Records[0].body);
    const startOfDay = triggerMessage.startOfDay ? triggerMessage.startOfDay * 1000 : START_OF_YESTERDAY;
    
    const formattedDate = dateFns.format(triggerMessage.startOfDay ? new Date(triggerMessage.startOfDay*1000) : day, 'YYYY-MM-DD');
    
    const REPO_SUB_DIRECTORY = `${process.env.SLACK_CHANNEL_NAME}/${new Date().getFullYear()}/${dateFns.format(day, 'MM')}`;

    console.log(`function triggered with: ${triggerMessage.latest}`);

    if(Math.floor(triggerMessage.latest) * 1000 < startOfDay) {
      console.log('reached to the start of yesterday', triggerMessage.latest);
      return {};
    }

    console.log('getting channel logs');
    const response = await getChannelLogs({ latest: triggerMessage.latest });

    const lastMessage = response.messages[response.messages.length - 1];

    const messages = getMessagesUntil(response.messages, startOfDay);
    const formattedMessages = await formatMessages(messages.filter(isValid));
    console.log('saving formatted messages to the database');
    await saveToDatabase({
      messages: formattedMessages,
      date: formattedDate
    });

    console.log(isEarlierThan(lastMessage), startOfDay);
    if(response.has_more && !isEarlierThan(lastMessage, startOfDay)) {
      console.log('is going to take more than one request to get all the data');
      const nextLatest = lastMessage.ts;

      const snsMessage = { latest: nextLatest, };
      if(triggerMessage.startOfDay) {
        snsMessage.startOfDay = triggerMessage.startOfDay;
      }

      console.log(`scheduling an SQS message for next invocation with: ${snsMessage}`);
      const params = {
        MessageBody: JSON.stringify(snsMessage),
        QueueUrl: process.env.AWS_SQS_URL
      };
      await sendSQSMessage(params);
      return {};
    } else {
      console.log('getting messages from the database');
      const fileName = `/tmp/text-${context.awsRequestId}.txt`;
      // retrieve messages from dynamodb
      let messagesFromDB = await getMessagesFromDatabase(formattedDate);
      messagesFromDB = messagesFromDB.map(messageObjToText);
      
      // TODO: set item in db as completed

      // append the messages we just requested
      messagesFromDB.push(...formattedMessages.map(messageObjToText));

      // slack API returns latest messages first so we reverse the array to have them in chronological order
      console.log('writing messages file to disk');
      await writeFile(fileName, new Buffer(messagesFromDB.reverse().join('')));

      if(process.env.GITHUB_REMOTE) {
        console.log('pushing the file to github');
        await saveFileToGitHub(fileName, `${repoDirectory}/${REPO_SUB_DIRECTORY}`, formattedDate, gitParams, repoDirectory);
      }
    }
  } catch (err) {
    console.log('there was an error', err);
    throw err;
  }
};

function isEarlierThan(message, start) {
  return Math.floor(message.ts) * 1000 < start;
}

async function formatMessages(messages) {
  // TODO: get unique list of users
  // pre-load info for that list to the usersMap then continue as normal
  return Promise.all(messages.map(message => formatMessage(message)));
}

async function formatMessage({ ts, text, ms, type, user, replies, thread_ts, client_msg_id}) {
  // TODO: check if the text contains a user id, if yes then replace it
  // TODO: map reactions those are invaluable...
  const formatted = {
    ts, text, ms, type, user, replies, thread_ts, client_msg_id
  };
  formatted.ms = Math.floor(ts) * 1000;
  try {
    const response = await getUserInfo(user);
    if(!response.user) {
      console.log('no info response', response);
      return {};
    }
    formatted.user = `${response.user.real_name} (@${response.user.name})`;
    return formatted;
  } catch(err) {
    console.log(err);
    return {};
  }
}

function messageObjToText(obj) {
  return `${formatToTimeZone(new Date(obj.ts*1000), 'HH:mm:ss', { timeZone: 'America/Argentina/Buenos_Aires' })} - ${obj.user}:\n${obj.text}\n`;
}

async function getUserInfo(userId){
  if(usersMap[userId]) {
    return Promise.resolve(usersMap[userId]);
  }
  const response = await request({
    uri: `${process.env.SLACK_API_URL}/users.info`,
    qs: {
      token: process.env.SLACK_TOKEN,
      user: userId
    },
    json: true
  });
  usersMap[userId] = response;
  return response;
}

function isValid(messageObj) {
  if (messageObj) {
    if(messageObj.subtype && (messageObj.subtype === 'channel_join' || messageObj.subtype === 'channel_leave' || messageObj.subtype === 'bot_message')) {
      return false;
    }
    if(messageObj.text === '' || typeof messageObj.text === undefined || messageObj.text === null) {
      return false;
    }
  }

  return true;
}

function getMessagesUntil(messages, start) {
  return messages.filter(message => Math.floor(message.ts) * 1000 >= start);
}

async function getChannelLogs({ latest }) {
  return request({
    uri: `${process.env.SLACK_API_URL}/channels.history`,
    qs: {
      token: process.env.SLACK_TOKEN,
      channel: process.env.SLACK_CHANNEL,
      latest
    },
    json: true
  });
}

async function writeFile(filePath, buffer) {
  const writeStream = fs.createWriteStream(filePath);
  return new Promise((resolve, reject) => {
    eos(streamifier.createReadStream(buffer).pipe(writeStream), (err) => {
      if (err) {
        return reject(err);
      }
      return resolve();
    });
  });
}
