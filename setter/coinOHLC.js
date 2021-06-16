const { getCoinList } = require('../getter/coinList')
const { getCoinPrice24hr } = require('../getter/coinPrice24hr')
const { getCoinOHLC24hr, getCoinOHLC7day, getCoinOHLC30day } = require('../getter/coinOHLC')
const SEG_SIZE = 30
const THIRTY_MIN = 30 * 60 * 1000
const FOUR_HR = 4 * 60 * 60 * 1000
const TWELEVE_HR = 12 * 60 * 60 * 1000
const ONE_DAY = 24 * 60 * 60 * 1000
const SEVEN_DAY = 7 * 24 * 60 * 60 * 1000
const THIRTY_DAY = 30 * 24 * 60 * 60 * 1000

const createCoinOHLC = async () => {
  const { dynamoClient } = global

  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)

  const empty_object = {}

  for (let i = 1; i <= segCounts; i++) {
    dynamoClient.put({
      TableName: 'CoinOHLC24hr',
      Item: {
        segment: i,
        ohlc: empty_object
      },
      ConditionExpression: 'attribute_not_exists(#seg)',
      ExpressionAttributeNames: { '#seg': 'segment' }
    }).promise()
    dynamoClient.put({
      TableName: 'CoinOHLC7day',
      Item: {
        segment: i,
        ohlc: empty_object
      },
      ConditionExpression: 'attribute_not_exists(#seg)',
      ExpressionAttributeNames: { '#seg': 'segment' }
    }).promise()
    dynamoClient.put({
      TableName: 'CoinOHLC30day',
      Item: {
        segment: i,
        ohlc: empty_object
      },
      ConditionExpression: 'attribute_not_exists(#seg)',
      ExpressionAttributeNames: { '#seg': 'segment' }
    }).promise()
  }
}

const updateCoinOHLC24hr = async () => {
  const { dynamoClient } = global

  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)

  const expireTime = (new Date()).getTime() - ONE_DAY
  const endTime = Math.floor((new Date()).getTime() / THIRTY_MIN) * THIRTY_MIN
  const startTime = endTime - THIRTY_MIN

  for (let i = 1; i <= segCounts; i++) {
    const coinPrice24hrItem = await getCoinPrice24hr(i)
    const { coinPrice } = coinPrice24hrItem

    const coinOHLC = await getCoinOHLC24hr(i)
    let ohlc = {}
    if (coinOHLC != undefined || Object.keys(coinOHLC.ohlc).length == 0) {
      ohlc = coinOHLC.ohlc
    }

    for (const [key, value] of Object.entries(coinPrice)) {
      const prices = value.filter(el => {
        return new Date(el.last_updated).getTime() >= startTime &&
               new Date(el.last_updated).getTime() < endTime
      }).map(el => el.current_price)
      if (prices.length == 0) { continue }

      const data = {
        datetime: endTime,
        o: prices[0],
        h: Math.max(...prices),
        l: Math.min(...prices),
        c: prices[prices.length - 1]
      }

      if (key in ohlc) {
        ohlc[key] = ohlc[key].filter(el => {
          return el.datetime >= expireTime
        })
        ohlc[key].push(data)
      } else {
        ohlc[key] = [data]
      }
    }

    dynamoClient.update({
      TableName: 'CoinOHLC24hr',
      Key: { segment: i },
      UpdateExpression: 'SET ohlc = :ohlc',
      ExpressionAttributeValues: { ':ohlc': ohlc }
    }).promise()
  }
}

const updateCoinOHLC7day = async () => {
  const { dynamoClient } = global

  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)

  const expireTime = (new Date()).getTime() - SEVEN_DAY
  const endTime = Math.floor((new Date()).getTime() / FOUR_HR) * FOUR_HR
  const startTime = endTime - FOUR_HR

  for (let i = 1; i <= segCounts; i++) {
    const coinOHLC24hr = await getCoinOHLC24hr(i)
    if (coinOHLC24hr == undefined || Object.keys(coinOHLC24hr.ohlc).length == 0) {
      continue
    }
    const ohlc24hr = coinOHLC24hr.ohlc

    const coinOHLC7day = await getCoinOHLC7day(i)
    let ohlc7day = {}
    if (coinOHLC7day != undefined) {
      ohlc7day = coinOHLC7day.ohlc
    }

    for (const [key, value] of Object.entries(ohlc24hr)) {
      const ohlc = value.filter(el => {
        return el.datetime >= startTime &&
               el.datetime < endTime
      })
      if (ohlc.length == 0) { continue }

      const high = ohlc.map(el => el.h)
      const low = ohlc.map(el => el.l)

      const data = {
        datetime: endTime,
        o: ohlc[0].o,
        h: Math.max(...high),
        l: Math.min(...low),
        c: ohlc[ohlc.length - 1].c
      }

      if (key in ohlc7day) {
        ohlc7day[key] = ohlc7day[key].filter(el => {
          return el.datetime >= expireTime
        })
        ohlc7day[key].push(data)
      } else {
        ohlc7day[key] = [data]
      }
    }

    dynamoClient.update({
      TableName: 'CoinOHLC7day',
      Key: { segment: i },
      UpdateExpression: 'SET ohlc = :ohlc',
      ExpressionAttributeValues: { ':ohlc': ohlc7day }
    }).promise()
  }
}

const updateCoinOHLC30day = async () => {
  const { dynamoClient } = global

  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)

  const expireTime = (new Date()).getTime() - THIRTY_DAY
  const endTime = Math.floor((new Date()).getTime() / TWELEVE_HR) * TWELEVE_HR
  const startTime = endTime - TWELEVE_HR

  for (let i = 1; i <= segCounts; i++) {
    const coinOHLC7day = await getCoinOHLC7day(i)
    if (coinOHLC7day == undefined || Object.keys(coinOHLC7day.ohlc).length == 0) {
      continue
    }
    const ohlc7day = coinOHLC7day.ohlc

    const coinOHLC30day = await getCoinOHLC30day(i)
    let ohlc30day = {}
    if (coinOHLC30day != undefined) {
      ohlc30day = coinOHLC30day.ohlc
    }

    for (const [key, value] of Object.entries(ohlc7day)) {
      const ohlc = value.filter(el => {
        return el.datetime >= startTime &&
               el.datetime < endTime
      })
      if (ohlc.length == 0) { continue }

      const high = ohlc.map(el => el.h)
      const low = ohlc.map(el => el.l)

      const data = {
        datetime: endTime,
        o: ohlc[0].o,
        h: Math.max(...high),
        l: Math.min(...low),
        c: ohlc[ohlc.length - 1].c
      }

      if (key in ohlc30day) {
        ohlc30day[key] = ohlc30day[key].filter(el => {
          return el.datetime >= expireTime
        })
        ohlc30day[key].push(data)
      } else {
        ohlc30day[key] = [data]
      }
    }

    dynamoClient.update({
      TableName: 'CoinOHLC30day',
      Key: { segment: i },
      UpdateExpression: 'SET ohlc = :ohlc',
      ExpressionAttributeValues: { ':ohlc': ohlc30day }
    }).promise()
  }
}

module.exports = {
  createCoinOHLC,
  updateCoinOHLC24hr,
  updateCoinOHLC7day,
  updateCoinOHLC30day
}
