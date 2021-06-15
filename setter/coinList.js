const fetch = require('node-fetch')
const { dynamoClient } = global
const LIST_URL = 'https://api.coingecko.com/api/v3/coins/list'

// 從 Coingecko 爬取資料並更新 "CoinList" 這個 Table
const updateCoinList = async () => {
  // 從 Coingecko 取得當前最新的 coin list
  const response = await fetch(LIST_URL)
  const coinList = await response.json()
  const coinPriceSegment = {}
  coinList.forEach((coin, index) => {
    coinPriceSegment[coin.id] = Math.ceil((index + 1) / 30)
  })
  
  // 新增隔天使用的 CoinList Item 到 table 中
  let day = new Date()
  day.setHours(0, 0, 0, 0)
  day.setDate(day.getDate() + 1)
  await dynamoClient.put({
    TableName: 'CoinList',
    Item: {
      dateTime: day.getTime(),  // primary key (datetime on 12 a.m that day)
      coinPriceSegment: coinPriceSegment
    }
  }).promise()
    .then(() => console.log('successfully updated CoinList'))
    .catch(error => {
      console.log('fail to update CoinList')
      console.log(error)
    })
}

module.exports = {
  updateCoinList
}