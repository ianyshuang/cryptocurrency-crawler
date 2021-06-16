const AWS = require('aws-sdk')
const cron = require('node-cron')

require('dotenv').config()

const dynamoClient = new AWS.DynamoDB.DocumentClient({
  apiVersion: '2012-08-10',
  region: 'ap-northeast-1',
  accessKeyId: process.env.DYNAMO_KEY,
  secretAccessKey: process.env.DYNAMO_SECRET
})
global.dynamoClient = dynamoClient

// these methods need to be required after dynamoClient put into "global"
const { createCoinList } = require('./setter/coinList')
const { createCoinMarket } = require('./setter/coinMarket')
const { createCoinPrice24hr, createCoinPrice7day, updateCoinPrice24hr, updateCoinPrice7day } = require('./setter/coinPrice')
const { createCoinOHLC, updateCoinOHLC24hr, updateCoinOHLC7day, updateCoinOHLC30day } = require('./setter/coinOHLC')

const run = async() => {

  let cronOptions = {
    scheduled: true,
    timezone: 'Asia/Taipei'
  }

  // 每天 23:17 時更新明天要用的 CoinList
  cron.schedule('17 23 * * *', createCoinList, cronOptions)

  // 每天 23:27 時更新明天要用到的 CoinPrice24hr 的兩個 items
  cron.schedule('27 23 * * *', createCoinPrice24hr, cronOptions)

  // 每天 23:37 時更新明天要用到的 CoinPrice7day 的 item
  cron.schedule('37 23 * * *', createCoinPrice7day, cronOptions)

  // 每天 23:47 時更新 CoinOHLC*
  cron.schedule('47 23 * * *', createCoinOHLC, cronOptions)

  // 每 5 分鐘 (0/5 分時) 更新 CoinMarket
  cron.schedule('*/5 * * * *', createCoinMarket, cronOptions)

  // 每 10 分鐘 (3 結尾) 更新
  cron.schedule('3,13,23,33,43,53 * * * * *', updateCoinPrice24hr, cronOptions)

  // 每小時（XX:08 時）更新
  cron.schedule('8 * * * *', updateCoinPrice7day, cronOptions)

  

}

run()

