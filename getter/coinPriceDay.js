// 從 Dynamo 取得最新的 CoinList
const getCoinPriceDay = async (seg) => {
  const now = new Date()
  const nowTime = now.getTime()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()
  const time = nowTime >= dateMidTime ? dateMidTime : dateStartTime

  const data = await dynamoClient.get({
    TableName: 'CoinPriceDay',
    Key: {
      segment: seg,
      time: time
    }
  }).promise()

  return data.Item
}

module.exports = {
  getCoinPriceDay
}