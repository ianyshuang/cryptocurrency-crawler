const fetch = require('node-fetch')
const { getCoinList } = require('../getter/coinList')
const { dynamoClient } = global
const MARKET_URL = 'https://api.coingecko.com/api/v3/coins/markets'

// 從 Coingecko 爬取資料並更新 "CoinMarket" 這個 Table
const updateCoinMarket = async () => {
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
    let queryString = `vs_currency=usd&order=market_cap_desc&sparkline=false&ids=${idString}&price_change_percentage=1h%2C24h%2C7d`

    // fetch data and concat to "coinMarketList"
    let response = await fetch(`${MARKET_URL}?${queryString}`)
    let data = await response.json()
    coinMarketList = coinMarketList.concat(data)
  }
  console.timeEnd('fetch data')

  // sort data with "market_cap" field in descending order
  console.time('sorting')
  coinMarketList.sort((prev, next) => next.market_cap - prev.market_cap)
  console.timeEnd('sorting')

  // 更新 "CoinMarket" (by page)
  const now = new Date()
  const time = now.getTime()
  const itemData = []
  const pages = Math.ceil(coinMarketList.length / 100)
  for (let i = 0; i < pages; i++) {
    let start = i * 100
    let end = Math.min((i + 1) * 100, coinMarketList.length)
    itemData.push({
      time: time,
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

module.exports = {
  updateCoinMarket
}