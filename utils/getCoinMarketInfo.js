const fetch = require('node-fetch')
const { getCoinList } = require('../getter/coinList')
const MARKET_URL = 'https://api.coingecko.com/api/v3/coins/markets'

module.exports = async () => {
  const coinListItem = await getCoinList()
  const { coinPriceSegment } = coinListItem
  const coinIdList = Object.keys(coinPriceSegment)

  const coinPerReq = 400
  const iteration = Math.ceil(coinIdList.length / coinPerReq)
  const promises = []
    for (let i = 0; i < iteration; i++) {
      let start = i * coinPerReq;
      let end = Math.min((i + 1) * coinPerReq, coinIdList.length)
      let ids = coinIdList.slice(start, end)
      let idString = ids.join('%2C')
      console.log(`fetching ${start} ~ ${end}`)
      
      // set the range of coins to fetch
      let queryString = `vs_currency=usd&ids=${idString}&order=market_cap_desc&sparkline=false`

      // fetch data and concat to "coinPriceList"
      let response = fetch(`${MARKET_URL}?${queryString}`)
      promises.push(response)
  }

  const responses = await Promise.all(promises)
    .then(values => values)
    .catch(error => console.log(error))

  return responses
}
