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
const { getCoinList } = require('./getter/coinList')
const { updateCoinList } = require('./setter/coinList')
const { updateCoinMarket } = require('./setter/coinMarket')
const { createCoinPriceDay, updateCoinPriceDay } = require('./setter/coinPrice')
const { createCoinOHLC, updateCoinOHLC } = require('./setter/coinOHLC')


const getCoinMarketInfo = async (url) => {
  const coinListItem = await getCoinList()
  const { coinIdList } = coinListItem

  const coinPerReq = 400
  const iteration = Math.ceil(coinIdList.length / coinPerReq)
  const promises = []
    for (let i = 0; i < iteration; i++) {
      let start = i * coinPerReq;
      let end = Math.min((i + 1) * coinPerReq, coinIdList.length)
      let ids = coinIdList.slice(start, end)
      let idString = ids.join('%2C')
      console.log(`fetching ${start} ~ ${end}`)
      
      // set the range of coins to fetch
      let queryString = `vs_currency=usd&ids=${idString}&order=market_cap_desc&sparkline=false`

      // fetch data and concat to "coinPriceList"
      let response = fetch(`${url}?${queryString}`)
      promises.push(response)
  }

  const responses = await Promise.all(promises)
    .then(values => values)
    .catch(error => console.log(error))

  return responses
}

const run = async() => {
  // updateCoinList()
  // updateCoinMarket()
  
  // createCoinPriceDay()
  // updateCoinPriceDay()

  // createCoinOHLC()
  // updateCoinOHLC()
}

run()

