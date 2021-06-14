const { getCoinList } = require('../getter/coinList')
const getCoinMarketInfo = require('../utils/getCoinMarketInfo')
const SEG_SIZE = 30

// 建立 CoinPriceDay 的 items
const createCoinPriceDay = async () => {
  const { dynamoClient } = global
  
  // 取得 coinList 資料
  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)

  // 計算 12 am / 12 pm 的 timestamp
  const now = new Date()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()

  // 建立每個 segment 的 item
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)
  const itemList = []
  for (let i = 1; i <= segCounts; i++) {
    itemList.push({ segment: i, coinPrice: {} })
  }
  for (let coin in coinPriceSegment) {
    let segment = coinPriceSegment[coin]
    itemList[segment - 1]['coinPrice'][coin] = []
  }
  
  for (let item of itemList) {
    dynamoClient.put({
      TableName: 'CoinPriceDay',
      Item: {
        ...item,
        time: dateStartTime
      }
    }).promise()
      .then(() => console.log(`successfully create segment ${item['segment']} - 1`))
      .catch(error => console.log(error))

    dynamoClient.put({
      TableName: 'CoinPriceDay',
      Item: {
        ...item,
        time: dateMidTime
      }
    }).promise()
      .then(() => console.log(`successfully create segment ${item['segment']} - 2`))
      .catch(error => console.log(error))
  }
}

// 從 Coingecko 爬取資料並更新 "CoinPriceFiveMin" 這個 Table
const updateCoinPriceDay = async () => {
  const { dynamoClient } = global
  const coinListItem = await getCoinList()
  const coinCounts = Object.keys(coinListItem.coinPriceSegment).length

  // 設定時間
  const now = new Date()
  const nowTime = now.getTime()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()
  const time = nowTime >= dateMidTime ? dateMidTime : dateStartTime
  
  // 從 coingecko 爬資料
  console.time('fetch coin market')
  const responses = await getCoinMarketInfo()
  console.timeEnd('fetch coin market')
  const newCoinPrice = {}
  for (let res of responses) {
    let data = await res.json()
    data.forEach(d => {
      newCoinPrice[d.id] = {
        current_price: d.current_price,
        last_updated: d.last_updated
      }
    })
  }

  // // 每 30 個 coins 為一個 segment
  const segCounts = Math.ceil(coinCounts / SEG_SIZE)
  for (let i = 1; i <= segCounts; i++) {
    let data = await dynamoClient.get({
      TableName: 'CoinPriceDay',
      Key: { segment: i, time: time }
    }).promise()
    
    let item = data.Item
    let oldCoinPrice = item.coinPrice
    for (let coin in oldCoinPrice) {
      oldCoinPrice[coin].push(newCoinPrice[coin])
    }

    dynamoClient.update({
      TableName: 'CoinPriceDay',
      Key: { segment: i, time: time },
      UpdateExpression: 'SET coinPrice = :coinPrice',
      ExpressionAttributeValues: { ':coinPrice': oldCoinPrice }
    }).promise()
      .then(() => console.log(`successfully update CoinPriceDay segment ${i}`))
      .catch(error => {
        console.log(`fail to update CoinPriceDay segment ${i}`)
        console.log(error)
      })
  }
}

module.exports = {
  createCoinPriceDay,
  updateCoinPriceDay
}