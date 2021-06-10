// 從 Dynamo 取得最新的 CoinList
const getCoinList = async () => {
  const day = new Date()
  day.setHours(0, 0, 0, 0)

  const data = await dynamoClient.get({
    TableName: 'CoinList',
    Key: { dateTime: day.getTime() }
  }).promise()

  return data.Item.coins
}

module.exports = {
  getCoinList
}