const fetch = require('node-fetch')
const { dynamoClient } = global

// 從 Coingecko 爬取資料並更新 "CoinList" 這個 Table
const updateCoinList = async (url) => {
  // 從 Coingecko 取得當前最新的 coin list
  const response = await fetch(url)
  const data = await response.json()
  const coinIdList = data.map(d => d.id)
  coinIdList.sort((prev, next) => (prev > next) ? 1 : -1) // sort coin ids alphabetically
  
  // 新增 item 到 "CoinList" 中
  let day = new Date()
  day.setHours(0, 0, 0, 0)
  await dynamoClient.put({
    TableName: 'CoinList',
    Item: {
      dateTime: day.getTime(),  // primary key (datetime on 12 a.m that day)
      coins: coinIdList
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