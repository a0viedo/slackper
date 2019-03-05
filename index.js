'use strict';

const { spawn } = require('child_process');
const path = require('path');
const streamifier = require('streamifier');
const eos = require('end-of-stream');
const AWS = require('aws-sdk');
const sqs = new AWS.SQS({region : 'eu-west-1'});
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const dateFns = require('date-fns');
const request = require('request-promise');
const fs = require('fs');
const util = require('util');

const day = dateFns.subDays(new Date(),1);
const today = dateFns.format(day, 'YYYY-MM-DD'); //.toISOString().substring(0, 10);
const DAY_IN_MILLISECONDS = 86400000;
const START_OF_YESTERDAY = process.env.START_TIME ? Number(process.env.START_TIME) : dateFns.startOfDay(day);
const REPO_SUB_DIRECTORY = `${process.env.SLACK_CHANNEL_NAME}/${new Date().getFullYear()}/${dateFns.format(day, 'MM')}`;
let REPO_DIRECTORY;
let GIT_PARAMS;
const usersMap = {};
module.exports.handler = async function (event, context) {
  REPO_DIRECTORY = `/tmp/${context.awsRequestId}`;
  GIT_PARAMS = `--work-tree ${REPO_DIRECTORY} --git-dir ${REPO_DIRECTORY}/.git`;
  console.dir(event, {depth: 7});

  try {
    const triggerMessage = JSON.parse(event.Records[0].body);
    console.log(`function triggered with: ${triggerMessage.latest}`);

    if(Math.floor(triggerMessage.latest) * 1000 < START_OF_YESTERDAY) {
      console.log('reached to the start of yesterday', triggerMessage.latest);
      return {};
    }

    console.log('getting channel logs');
    const response = await getChannelLogs({ latest: triggerMessage.latest });

    const lastMessage = response.messages[response.messages.length - 1];
    // console.log(result);
    
    console.log('isnotbeyondyesterday', isNotBeyondYesterday(lastMessage));
    console.log(response.has_more);

    debugger;
    const ydaymessages = getMessagesUntilYesterday(response.messages);
    const formattedMessages = await formatMessages(ydaymessages.filter(isValid));
    if(response.has_more && isNotBeyondYesterday(lastMessage)) {
      console.log('is going to take more than one request to get all the data');
      const nextLatest = lastMessage.ts;
      // save formattedMessages to dynamoDB
      console.log('saving formatted messages to the database');
      // console.dir(formattedMessages, {depth: 7});
      debugger;
      await saveToDatabase({
        messages: formattedMessages,
        date: today
      });

      console.log(`scheduling an SQS message for next invocation with latest ${nextLatest}`);
      const params = {
        MessageBody: JSON.stringify({ latest: nextLatest, }),
        QueueUrl: process.env.AWS_SQS_URL
      };
      await sendMessage(params);
      return {};
    } else {
      debugger;
      console.log('getting messages from the database');
      const fileName = `/tmp/${today}.txt`;
      // retrieve messages from dynamodb
      const messages = await getMessagesFromDatabase(today);
      
      // TODO: check if messages contain a user id, if yes then replace it

      // append the messages we just requested
      messages.push(...formattedMessages.map(messageObjToText));

      // slack API returns latest messages first so we reverse the array to have them in chronological order
      console.log('writing messages file to disk');
      await writeFile(fileName, new Buffer(messages.reverse().join('')));
      console.log('pushing the file to github');
      await saveFileToGitHub(fileName);
    }
  } catch (err) {
    console.log('there was an error', err);
    return {};
  }
};

function isNotBeyondYesterday(message) {
  return Math.floor(message.ts) * 1000 > START_OF_YESTERDAY;
}

async function formatMessages(messages) {
  // TODO: get unique list of users
  // pre-load info for that list then continue as normal
  return Promise.all(messages.map(message => formatMessage(message)));
}

async function formatMessage({ ts, text, ms, type, user}) {
  // return new Promise(async (resolve, reject) => {

  //   const formatted = {...message};
  //   formatted.ms = Math.floor(message.ts) * 1000;
  //   getUserInfo(message.user)
  //     .then(({ user: userInfo }) => {
  //       formatted.user = `${userInfo.real_name} (@${userInfo.name})`;
  //       console.log('finished formatting user', message.user);
  //       resolve(formatted);
  //     });

  // });

  // return message;

  // TODO: map reactions those are invaluable...
  const formatted = {
    ts, text, ms, type, user
  };
  formatted.ms = Math.floor(ts) * 1000;
  try {
    const response = await getUserInfo(user);
    if(!response.user) {
      console.log('no info response', response);
      return {};
    }
    formatted.user = `${response.user.real_name} (@${response.user.name})`;
    // console.log('finished formatting user', user);
    return formatted;
  } catch(err) {
    console.log(err);
  }

}

function messageObjToText(obj) {
  return `${dateFns.format(new Date(obj.ts*1000), 'HH:mm:ss')} - ${obj.user}:\n${obj.text}\n`;
}

async function getMessagesFromDatabase(date) {
  const params = {
    TableName: process.env.AWS_DYNAMODB_TABLE,
    Key: {
      slackTeam: `${process.env.SLACK_TEAM}-${date}`
    },
  };

  return new Promise((resolve, reject) => {
    // fetch todo from the database
    dynamoDb.get(params, (err, result) => {
      if(err) {
        return reject(err);
      }
      if(!result || !result.Item) {
        return resolve([]);
      }
      return resolve(result.Item.messages.map(messageObjToText));
    });
  });
}

async function saveToDatabase({ latest, date, messages }){
  const params = {
    TableName: process.env.AWS_DYNAMODB_TABLE,
    Key: {
      slackTeam: `${process.env.SLACK_TEAM}-${date}`
    },
    ReturnValues: 'ALL_NEW',
    UpdateExpression: 'set #messages = list_append(if_not_exists(#messages, :empty_list), :message)',
    ExpressionAttributeNames: {
      '#messages': 'messages'
    },
    ExpressionAttributeValues: {
      ':message': messages,
      ':empty_list': []
    }
  };

  return new Promise((resolve, reject) => {
    // write the todo to the database
    dynamoDb.update(params, (err) => {
      if(err) {
        return reject(err);
      }
      return resolve();
    });
  });

}

async function getUserInfo(userId){
  if(usersMap[userId]) {
    return Promise.resolve(usersMap[userId]);
  }
  // console.log('requesting data for', userId, new Date());
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
    if(messageObj.text === '' || typeof messageObj.text === undefined || typeof messageObj.text === null) {
      return false;
    }
  }

  return true;
}

function getMessagesUntilYesterday(messages) {
  return messages.filter(message => Math.floor(message.ts) * 1000 >= START_OF_YESTERDAY);
}

async function sendMessage(params) {
  return new Promise((resolve, reject) => {
    sqs.sendMessage(params, (err) => {
      if(err) {
        console.log(err);
        return reject(err);
      }
      return resolve();
    });
  });
}

function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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

// function getEndTime(){
//   const date = new Date();
//   const msIntoToday = date.getHours() * 60 * 60 * 1000 + date.getMinutes() * 60 * 1000 + date.getSeconds() * 1000;
//   return date.getTime() - msIntoToday;
// }

async function saveFileToGitHub(filePath) {
  console.log('cloning repository');
  await gitClone();
  // const complete = getRepoSubDirectory();
  const destinationPath = `${REPO_DIRECTORY}/${REPO_SUB_DIRECTORY}/${path.basename(filePath)}`;
  console.log('command', `mkdir -p ${REPO_DIRECTORY}/${REPO_SUB_DIRECTORY} && cp ${filePath} ${destinationPath}`);
  debugger;
  console.log('configuring git');
  await spawnPromise(`git config --global user.name "${process.env.GITHUB_USER}"`);
  await spawnPromise(`git config --global user.email "${process.env.GITHUB_USER_EMAIL}"`);
  console.log('creating a directory');
  await spawnPromise(`mkdir -p ${REPO_DIRECTORY}/${REPO_SUB_DIRECTORY}`);
  console.log('copying the file to the repository directory');
  await spawnPromise(`cp ${filePath} ${destinationPath}`);
  
  console.log('comitting file');
  await gitCommit(destinationPath, today);
  console.log('pushing file');
  await gitPush();
}

// function getRepoSubDirectory() {
//   const year = new Date().getFullYear();
//   const month = new Date().getMonth();
//   // const day = ('0' + new Date().getDate()).slice(-2);
//   return `${SLACK_CHANNEL_NAME}/${year}/${month}`
// }



async function gitClone() {
  await spawnPromise(`git clone ${process.env.GITHUB_REMOTE} ${REPO_DIRECTORY}`);
}

async function gitAdd(filePath) {
  await spawnPromise(`git ${GIT_PARAMS} add ${filePath}`);
}

async function gitCommit(filePath, commitMessage) {
  await gitAdd(filePath);
  await spawnPromise(`git ${GIT_PARAMS} commit -m ${commitMessage}`);
}

async function gitPush() {
  await spawnPromise(`git ${GIT_PARAMS} push`);
}

async function spawnPromise(cmd) {
  console.log('spawnPromise', cmd);
  const splittedCmd = cmd.split(' ');
  return new Promise((resolve, reject) => {
    const firstCommand = splittedCmd.shift();
    console.log('firstCommand', firstCommand);
    console.log(splittedCmd);
    const p = spawn(firstCommand, splittedCmd, { stdio: 'inherit' });
    p.on('exit', (code) => {
      if (code === 0) {
        return resolve();
      }
      return reject(new Error(`failed with exit code ${code}`));
    });
    p.on('error', (err) => reject(err));
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
