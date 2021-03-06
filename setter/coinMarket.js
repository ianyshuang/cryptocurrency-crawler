const getCoinMarketInfo = require('../utils/getCoinMarketInfo')

// 從 Coingecko 爬取資料並更新 "CoinMarket" 這個 Table
const createCoinMarket = async () => {
  console.log('createCoinMarket')

  const responses = await getCoinMarketInfo()
  console.log('finish fetching from coingecko')

  let coinMarketList = []
  for (let res of responses) {
    let data = await res.json()
    coinMarketList = coinMarketList.concat(data)
  }


  // sort data with "market_cap" field in descending order
  coinMarketList.sort((prev, next) => next.market_cap - prev.market_cap)
  console.log('finish sorting by market_cap')

  // 更新 "CoinMarket" (by page)
  const now = new Date()
  const fiveMin = 1000 * 60 * 5 // 5 minutes in milliseconds
  const time = Math.round(now.getTime() / fiveMin) * fiveMin  // nearest XX:X0 / XX:X5 's timestamp
  const itemData = []
  const pages = Math.ceil(coinMarketList.length / 100)
  for (let i = 0; i < pages; i++) {
    let start = i * 100
    let end = Math.min((i + 1) * 100, coinMarketList.length)
    itemData.push({
      time: time,
      page: i + 1,
      coinMarket: coinMarketList.slice(start, end)
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

module.exports = {
  createCoinMarket
}