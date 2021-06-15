const getCoinOHLC24hr = async (seg) => {
  const now = new Date()
  const nowTime = now.getTime()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()
  const time = nowTime >= dateMidTime ? dateMidTime : dateStartTime

  const data = await dynamoClient.get({
    TableName: 'CoinOHLC24hr',
    Key: {
      segment: seg
    }
  }).promise()

  return data.Item
}

const getCoinOHLC7day = async (seg) => {
  const now = new Date()
  const nowTime = now.getTime()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()
  const time = nowTime >= dateMidTime ? dateMidTime : dateStartTime

  const data = await dynamoClient.get({
    TableName: 'CoinOHLC7day',
    Key: {
      segment: seg
    }
  }).promise()

  return data.Item
}

const getCoinOHLC30day = async (seg) => {
  const now = new Date()
  const nowTime = now.getTime()
  now.setHours(0, 0, 0, 0)
  const dateStartTime = now.getTime()
  now.setHours(12, 0, 0, 0)
  const dateMidTime = now.getTime()
  const time = nowTime >= dateMidTime ? dateMidTime : dateStartTime

  const data = await dynamoClient.get({
    TableName: 'CoinOHLC30day',
    Key: {
      segment: seg
    }
  }).promise()

  return data.Item
}

module.exports = {
  getCoinOHLC24hr,
  getCoinOHLC7day,
  getCoinOHLC30day
}