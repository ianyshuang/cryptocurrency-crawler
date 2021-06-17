const fetch = require('node-fetch')
const { dynamoClient } = global
const LIST_URL = 'https://api.coingecko.com/api/v3/coins/list'

// 從 Coingecko 爬取資料並更新 "CoinList" 這個 Table
const createCoinList = async () => {
  // 從 Coingecko 取得當前最新的 coin list
  const response = await fetch(LIST_URL)
  const coinList = await response.json()
  const coinPriceSegment = {}
  coinList.forEach((coin, index) => {
    coinPriceSegment[coin.id] = Math.ceil((index + 1) / 30)
  })

  // 取得今天的 CoinList
  let now = new Date()
  now.setHours(0, 0, 0, 0)
  let data = await dynamoClient.get({
    TableName: 'CoinList',
    Key: { dateTime: now.getTime() }
  }).promise()

  // 如果新抓下來的 coin 沒有在原本的 CoinList 才更動 segment
  let segment = data.Item.coinPriceSegment
  for (let coin in segment) {
    if (!segment[coin]) {
      segment[coin] = coinPriceSegment[coin]
    }
  }
  
  // 新增隔天使用的 CoinList Item 到 table 中
  now.setDate(now.getDate() + 1)
  await dynamoClient.put({
    TableName: 'CoinList',
    Item: {
      dateTime: now.getTime(),  // primary key (datetime on 12 a.m that day)
      coinPriceSegment: segment
    }
  }).promise()
    .then(() => console.log('successfully updated CoinList'))
    .catch(error => {
      console.log('fail to update CoinList')
      console.log(error)
    })
}

module.exports = {
  createCoinList
}