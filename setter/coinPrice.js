const { getCoinList } = require('../getter/coinList')
const getCoinMarketInfo = require('../utils/getCoinMarketInfo')
const SEG_SIZE = 30

// 建立 CoinPrice24hr 的 items
// 共建立兩個 items，一個 item 為 12 小時 (00:00 ~ 11:59 & 12:00 ~ 23:39)
// cron job 在前一天就先建立隔天的 items
const createCoinPrice24hr = async () => {
  const { dynamoClient } = global

  // 取得 coinList 資料
  const coinListItem = await getCoinList(false)
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)

  // 計算明天 12 am / 12 pm 的 timestamp
  const now = new Date()
  now.setDate(now.getDate() + 1)
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
      TableName: 'CoinPrice24hr',
      Item: {
        ...item,
        time: dateStartTime
      }
    }).promise()
      .then(() => console.log(`successfully create CoinPrice24hr segment ${item['segment']} - 1`))
      .catch(error => {
        console.log(`fail to create CoinPrice24hr segment ${item['segment']} - 1`)
        console.log(error)
      })

    dynamoClient.put({
      TableName: 'CoinPrice24hr',
      Item: {
        ...item,
        time: dateMidTime
      }
    }).promise()
      .then(() => console.log(`successfully create CoinPrice24hr segment ${item['segment']} - 2`))
      .catch(error => {
        console.log(`fail to create CoinPrice24hr segment ${item['segment']} - 2`)
        console.log(error)
      })
  }
}

// 建立 CoinPrice7day 的 item
// 每個 item 為 24 小時
// cron job 在前一天就先建立隔天的 item
const createCoinPrice7day = async () => {
  const { dynamoClient } = global
  const coinListItem = await getCoinList(false)
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)

  const now = new Date()
  now.setDate(now.getDate() + 1)
  now.setHours(0, 0, 0, 0)
  const time = now.getTime()

  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)
  const itemList = []
  for (let i = 1; i <= segCounts; i++) {
    itemList.push({ segment: i, time: time, coinPrice: {} })
  }
  for (let coin in coinPriceSegment) {
    let segment = coinPriceSegment[coin]
    itemList[segment - 1]['coinPrice'][coin] = []
  }

  for (let item of itemList) {
    dynamoClient.put({
      TableName: 'CoinPrice7day',
      Item: item
    }).promise()
      .then(() => console.log(`successfully create CoinPrice7day item segment ${item['segment']}`))
      .catch(error => {
        console.log(`fail to create CoinPrice7day item segment ${item['segment']}`)
        console.log(error)
      })
  }
}


// 從 Coingecko 爬取資料並更新 "CoinPrice24hr" 這個 table
// 更新頻率為 5 分鐘
const updateCoinPrice24hr = async () => {
  console.log('updateCoinPrice24hr')

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
  const responses = await getCoinMarketInfo()
  console.log('finish fetching from coingecko')

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
      TableName: 'CoinPrice24hr',
      Key: { segment: i, time: time }
    }).promise()
    
    let item = data.Item
    let oldCoinPrice = item.coinPrice
    for (let coin in oldCoinPrice) {
      let newEntry = newCoinPrice[coin]
      if (!newEntry) continue // coingecko 沒有提供這個 coin 的 market 資料

      if (oldCoinPrice[coin].length === 0) { // 目前沒有這個 coin 的 price
        oldCoinPrice[coin].push(newEntry)
      } else {
        let lastEntry = oldCoinPrice[coin][oldCoinPrice[coin].length - 1]
        if (lastEntry['last_updated'] !== newEntry['last_updated'] && lastEntry['current_price'] !== newEntry['current_price']) {
          oldCoinPrice[coin].push(newEntry)
        }
      }
    }

    dynamoClient.update({
      TableName: 'CoinPrice24hr',
      Key: { segment: i, time: time },
      UpdateExpression: 'SET coinPrice = :coinPrice',
      ExpressionAttributeValues: { ':coinPrice': oldCoinPrice }
    }).promise()
      .then(() => console.log(`successfully update CoinPrice24hr segment ${i}`))
      .catch(error => {
        console.log(`fail to update CoinPrice24hr segment ${i}`)
        console.log(error)
      })
  }
}

// 從 Coingecko 爬取資料並更新 "CoinPrice7day" 這個 table
// 更新頻率為 1 小時
const updateCoinPrice7day = async () => {
  console.log('updateCoinPrice7day')

  const { dynamoClient } = global
  const coinListItem = await getCoinList()
  const coinCounts = Object.keys(coinListItem.coinPriceSegment).length

  const now = new Date()
  now.setHours(0, 0, 0, 0)
  const time = now.getTime()

  // 從 coingecko 爬取資料
  const responses = await getCoinMarketInfo()
  console.log('finish fetching from coingecko')
  
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

  // 每 30 個 coins 為一個 segment
  const segCounts = Math.ceil(coinCounts / SEG_SIZE)
  for (let i = 1; i <= segCounts; i++) {
    let data = await dynamoClient.get({
      TableName: 'CoinPrice7day',
      Key: { segment: i, time: time }
    }).promise()

    let item = data.Item
    let oldCoinPrice = item.coinPrice
    for (let coin in oldCoinPrice) {
      let newEntry = newCoinPrice[coin]
      if (!newEntry) continue // coingecko 沒有提供這個 coin 的 market 資料

      if (oldCoinPrice[coin].length === 0) { // 目前沒有這個 coin 的 price
        oldCoinPrice[coin].push(newEntry)
      } else {
        let lastEntry = oldCoinPrice[coin][oldCoinPrice[coin].length - 1]
        if (lastEntry['last_updated'] !== newEntry['last_updated'] && lastEntry['current_price'] !== newEntry['current_price']) {
          oldCoinPrice[coin].push(newEntry)
        }
      }
    }

    dynamoClient.update({
      TableName: 'CoinPrice7day',
      Key: { segment: i, time: time },
      UpdateExpression: 'SET coinPrice = :coinPrice',
      ExpressionAttributeValues: { ':coinPrice': oldCoinPrice }
    }).promise()
      .then(() => console.log(`successfully update CoinPrice7day segment ${i}`))
      .catch(error => {
        console.log(`fail to update CoinPrice7day segment ${i}`)
        console.log(error)
      })
  }
}

module.exports = {
  createCoinPrice24hr,
  createCoinPrice7day,
  updateCoinPrice24hr,
  updateCoinPrice7day
}