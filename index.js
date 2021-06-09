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

// 從 Coingecko 爬取資料並更新 "CoinList" 這個 Table
const updateCoinList = async (url) => {
  // 從 Coingecko 取得當前最新的 coin list
  const response = await fetch(url)
  const data = await response.json()
  const coinIdList = data.map(d => d.id)

  // 更新 "CoinUpdateTime" 這個 Table
  console.time('CoinUpdateTime')
  let time = new Date().getTime()
  // 在 "CoinUpdateTime" 中，將 "CoinList" 最新版本的時間 append 到 "datetime" 這個 field 中
  await dynamoClient.update({
    TableName: 'CoinUpdateTime',
    Key: { table: 'CoinList' },
    UpdateExpression: 'SET #datetime = list_append(#datetime, :time)',
    ExpressionAttributeNames: { '#datetime': 'datetime' },
    ExpressionAttributeValues: { ':time': [time] }
  }).promise()
    .then(() => console.log('succcessfully updated CoinUpdateTime') )
    .catch(error => {
      console.log('fail to update CoinUpdateTime')
      console.log(error)
    })
  console.timeEnd('CoinUpdateTime')

  // 新增 item 到 "CoinList" 中
  await dynamoClient.put({
    TableName: 'CoinList',
    Item: {
      datetime: time,  // primary key
      coins: coinIdList
    }
  }).promise()
    .then(() => console.log('successfully updated CoinList'))
    .catch(error => {
      console.log('fail to update CoinList')
      console.log(error)
    })
}

// 從 Dynamo 取得最新的 CoinList
const getCoinList = async () => {
  // 從 CoinUpdateTime 中取得 CoinList 目前最新版本的 datetime (primary key)
  const coinUpdateTime = await dynamoClient.get({
    TableName: 'CoinUpdateTime',
    Key: { table: 'CoinList' }
  }).promise()
  const timeSeries = coinUpdateTime.Item.datetime
  const latestTime = timeSeries[timeSeries.length - 1]

  const data = await dynamoClient.get({
    TableName: 'CoinList',
    Key: { datetime: latestTime }
  }).promise()

  return data.Item.coins
}

// 從 Coingecko 爬取資料並更新 "CoinMarket" 這個 Table
const updateCoinMarket = async (url) => {
  // get coin list from database
  console.time('get coinList')
  const coinList = await getCoinList()
  console.timeEnd('get coinList')
  
  // 從 coinList 中每次取出 250 個 coins (API limit)
  // 向 coingecko 發 requests 取得 coin market 資料
  console.time('fetch data')
  const iteration = Math.ceil(coinList.length / 250)
  let coinMarketList = []
  for (let i = 0; i < iteration; i++) {
    let start = i * 250;
    let end = Math.min((i + 1) * 250, coinList.length)
    let ids = coinList.slice(start, end)
    let idString = ids.join('%2C')
    console.log(`fetching ${start} ~ ${end}`)
    
    // set the range of coins to fetch
    let queryString = `vs_currency=usd&&order=market_cap_desc&sparkline=false&ids=${idString}&price_change_percentage=1h%2C24h%2C7d`

    // fetch data and concat to "coinMarketList"
    let response = await fetch(`${url}?${queryString}`)
    let data = await response.json()
    coinMarketList = coinMarketList.concat(data)
  }
  console.timeEnd('fetch data')

  // sort data with "market_cap" field in descending order
  console.time('sorting')
  coinMarketList.sort((prev, next) => next.market_cap - prev.market_cap)
  console.timeEnd('sorting')

  // 更新 "CoinUpdateTime"
  // apppend 最新版本的時間到 CoinMarket 的 "datetime"
  console.time('update coinUpdateTime')
  const now = new Date()
  const time = now.getTime()
  
  await dynamoClient.update({
    TableName: 'CoinUpdateTime',
    Key: { table: 'CoinMarket' },
    UpdateExpression: 'SET #datetime = list_append(#datetime, :time)',
    ExpressionAttributeNames: { '#datetime': 'datetime' },
    ExpressionAttributeValues: { ':time': [time] }
  }).promise()
    .then(() => console.log('successfully update CoinUpdateTime') )
    .catch(error => {
      console.log('fail to update CoinUpdateTime')
      console.log(error)
    })
  console.timeEnd('update coinUpdateTime')

  // 更新 "CoinMarket" (by page)
  const itemData = []
  const pages = Math.ceil(coinMarketList.length / 100)
  for (let i = 0; i < pages; i++) {
    let start = i * 100
    let end = Math.min((i + 1) * 100, coinMarketList.length)
    itemData.push({
      datetime: time,
      page: i + 1,
      coins: coinMarketList.slice(start, end)
    })
  }

  for (let i = 0; i < itemData.length; i++) {
    dynamoClient.put({
      TableName: 'CoinMarket',
      Item: itemData[i]
    }).promise()
      .then(() => {
        console.log(`successfully write CoinMarket item page ${i+1}`)
      })
      .catch(error => {
        console.log(`fail to write CoinMarket item page ${i+1}`)
        console.log(error)
      })
  }
}



const run = async() => {
  const coinListUrl = 'https://api.coingecko.com/api/v3/coins/list'
  const coinMarketUrl = 'https://api.coingecko.com/api/v3/coins/markets'
  
  // updateCoinList(coinListUrl)
  // updateCoinMarket(coinMarketUrl)
}



run()

