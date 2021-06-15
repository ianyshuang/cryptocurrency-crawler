const fetch = require('node-fetch')
const AWS = require('aws-sdk')
const { inspect } = require('util')
const fs = require('fs')

require('dotenv').config()

const dynamoClient = new AWS.DynamoDB.DocumentClient({
  apiVersion: '2012-08-10',
  region: 'ap-northeast-1',
  accessKeyId: process.env.DYNAMO_KEY,
  secretAccessKey: process.env.DYNAMO_SECRET
})
global.dynamoClient = dynamoClient

// these methods need to be required after dynamoClient put into "global"
const { updateCoinList } = require('./setter/coinList')
const { updateCoinMarket } = require('./setter/coinMarket')
const { createCoinPriceDay, updateCoinPriceDay } = require('./setter/coinPrice')
const { createCoinOHLC, updateCoinOHLC24hr, updateCoinOHLC7day, updateCoinOHLC30day } = require('./setter/coinOHLC')


const run = async() => {
  // updateCoinList()
  // updateCoinMarket()
  
  // createCoinPriceDay()
  // updateCoinPriceDay()

  // createCoinOHLC()
  // updateCoinOHLC24hr()
  // updateCoinOHLC7day()
  updateCoinOHLC30day()
}

run()

