const fetch = require('node-fetch')
const { dynamoClient } = global
const LIST_URL = 'https://api.coingecko.com/api/v3/coins/list'

// 從 Coingecko 爬取資料並更新 "CoinList" 這個 Table
const updateCoinList = async () => {
  // 從 Coingecko 取得當前最新的 coin list
  const response = await fetch(LIST_URL)
  const data = await response.json()
  const coinIdList = data.map(coin => coin.id)
  const coinPriceSegment = {}
  coinIdList.forEach((coin, index) => {
    coinPriceSegment[coin] = Math.ceil((index + 1) / 30)
  })
  
  // 新增 item 到 "CoinList" 中
  let day = new Date()
  day.setHours(0, 0, 0, 0)
  await dynamoClient.put({
    TableName: 'CoinList',
    Item: {
      dateTime: day.getTime(),  // primary key (datetime on 12 a.m that day)
      coinIdList: coinIdList,
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