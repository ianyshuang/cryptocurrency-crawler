// 從 Dynamo 取得最新的 CoinList
const getCoinOHLC = async (seg) => {
  const now = new Date()
  const nowTime = now.getTime()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()
  const time = nowTime >= dateMidTime ? dateMidTime : dateStartTime

  const data = await dynamoClient.get({
    TableName: 'CoinOHLC',
    Key: {
      segment: seg
    }
  }).promise()

  return data.Item
}

module.exports = {
  getCoinOHLC
}