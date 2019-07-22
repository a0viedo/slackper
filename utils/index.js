'use strict';

const { spawn } = require('child_process');
const AWS = require('aws-sdk');
const sqs = new AWS.SQS({region: 'eu-west-1'}); // TODO: check if region is necessary, shouldn't serverless set that up?
const dynamoDb = new AWS.DynamoDB.DocumentClient();

module.exports.getMessagesFromDatabase = async function getMessagesFromDatabase(date) {
  const params = {
    TableName: process.env.AWS_DYNAMODB_TABLE,
    Key: {
      slackTeam: `${process.env.SLACK_TEAM}-${process.env.SLACK_CHANNEL_NAME}-${date}`
    },
  };

  const result = await dynamoDb.get(params).promise();
  return (!result || !result.Item) ? [] : result.Item.messages;
};

module.exports.saveToDatabase = async function saveToDatabase({ date, messages }){
  const params = {
    TableName: process.env.AWS_DYNAMODB_TABLE,
    Key: {
      slackTeam: `${process.env.SLACK_TEAM}-${process.env.SLACK_CHANNEL_NAME}-${date}`
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

  await dynamoDb.update(params).promise();
};

module.exports.setItemAsCompleted = async function setItemAsCompleted(date) {
  const params = {
    TableName: process.env.AWS_DYNAMODB_TABLE,
    Key: {
      slackTeam: `${process.env.SLACK_TEAM}-${process.env.SLACK_CHANNEL_NAME}-${date}`
    },
    ReturnValues: 'UPDATED_NEW',
    UpdateExpression: 'set completed = :completed',
    ExpressionAttributeValues: {
      ':completed': true
    }
  };

  await dynamoDb.update(params).promise();
};

module.exports.saveFileToGitHub = async function saveFileToGitHub(filePath, repoDirectory, date, gitParams, temporaryDirectory) {
  console.log('cloning repository');
  await gitClone(temporaryDirectory);

  const destinationPath = `${repoDirectory}/${date}.txt`;
  console.log('command', `mkdir -p ${repoDirectory} && cp ${filePath} ${destinationPath}`);

  console.log('configuring git');
  await spawnPromise(`git config --global user.name "${process.env.GITHUB_USER}"`);
  await spawnPromise(`git config --global user.email "${process.env.GITHUB_USER_EMAIL}"`);
  console.log('creating a directory');
  await spawnPromise(`mkdir -p ${repoDirectory}`);
  console.log('copying the file to the repository directory');
  await spawnPromise(`cp ${filePath} ${destinationPath}`);
  
  console.log('comitting file');
  await gitCommit(destinationPath, date, gitParams);
  console.log('pushing file');
  await gitPush(gitParams);
};

module.exports.sendSQSMessage = async function sendSQSMessage(params) {
  await sqs.sendMessage(params).promise();
};

async function gitClone(temporaryDirectory) {
  await spawnPromise(`git clone ${process.env.GITHUB_REMOTE} ${temporaryDirectory}`);
}

async function gitAdd(filePath, gitParams) {
  await spawnPromise(`git ${gitParams} add ${filePath}`);
}

async function gitCommit(filePath, commitMessage, gitParams) {
  await gitAdd(filePath, gitParams);
  await spawnPromise(`git ${gitParams} commit -m ${commitMessage}`);
}

async function gitPush(gitParams) {
  await spawnPromise(`git ${gitParams} push`);
}

async function spawnPromise(cmd) {
  console.log('spawnPromise', cmd);

  const splittedCmd = cmd.split(' ');
  return new Promise((resolve, reject) => {
    const p = spawn(splittedCmd.shift(), splittedCmd, { stdio: 'inherit' });
    p.on('exit', (code) => {
      if (code === 0) {
        return resolve();
      }
      return reject(new Error(`failed with exit code ${code}`));
    });
    p.on('error', (err) => reject(err));
  });
}
