// 從 Dynamo 取得 CoinList
// 如果 today 為 true 則取當天的 CoinList
// 如果 today 為 false 則取明天的 CoinList (有些 item 需提前一天建立)
const getCoinList = async (today = true) => {
  const day = new Date()
  day.setHours(0, 0, 0, 0)
  if (!today) day.setDate(day.getDate() + 1)

  const data = await dynamoClient.get({
    TableName: 'CoinList',
    Key: { dateTime: day.getTime() }
  }).promise()

  return data.Item
}

module.exports = {
  getCoinList
}