package Config

import "time"

const ConstProduction = true
const ConstRedisKey = "formula_"
const ConstRedisAPIHitKey = "credAPI_"
const ConstRedisAPIAccessToken = "accessTokenAPI_"
const ConstRedisAPIRefreshToken = "refreshTokenAPI_"
const ConstRedisExpiration = 24 * time.Hour

const ConstAPIReceiverPort = "31234"
const ConstAPIClientPort = "31122"
const ConstAPIAdminPort = "31110"

const ConstDefaultSalt = "#S@y4Bis4123!"
const ConstDefaultGroupID = "P1nt4rC@r!U5aha&"
const ConstDefaultClientID = "C3r!aC@riCu4n!!"

const ConstReloadToRedis = 3
const ConstProcessGlobal = "5s"
const ConstProcessDataDay = "3s"
const ConstProcessDataDate = "3s"
const ConstProcessDataRealtime = "5s"
