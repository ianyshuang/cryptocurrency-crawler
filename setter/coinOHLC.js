const { getCoinList } = require('../getter/coinList')
const { getCoinPriceDay } = require('../getter/coinPriceDay')
const SEG_SIZE = 30
const tenMin = 10 * 60 * 1000

const createCoinOHLC = async () => {
  const { dynamoClient } = global

  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)

  for (let i = 1; i <= segCounts; i++) {
    dynamoClient.put({
      TableName: 'CoinOHLC',
      Item: {
        segment: i,
        ohlc: {}
      }
    })
  }
}

const updateCoinOHLC = async () => {
  const { dynamoClient } = global

  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)
  const segCounts = Math.ceil(coinIdList.length / SEG_SIZE)

  const endTime = Math.floor((new Date).getTime() / tenMin) * tenMin
  const startTime = endTime - tenMin

  for (let i = 1; i <= segCounts; i++) {
    const coinPriceDayItem = await getCoinPriceDay(i)
    const { coinPrice } = coinPriceDayItem

    const coinOHLC = await getCoinOHLC(i)
    const { ohlc } = coinOHLC

    for (const [key, value] of Object.entries(coinPrice)) {
      const prices = value.filter(el => {
        return el.last_updated >= startTime &&
               el.last_updated < endTime
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
        ohlc[key].push(data)
      } else {
        ohlc[key] = [data]
      }
    }

    dynamoClient.put({
      TableName: 'CoinOHLC',
      Item: {
        segment: i,
        ohlc: ohlc
      }
    })
  }
}

module.exports = {
  createCoinOHLC,
  updateCoinOHLC
}
