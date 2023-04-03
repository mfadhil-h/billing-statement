package APICredential

import (
	"billing/Config"
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
)

func createNewToken(db *sql.DB, redisClient *redis.Client, goContext context.Context, incClientID string,
	incUsername string, incPassword string, incRemoteIPAddress string) (map[string]interface{}, string, string) {

	mapResponse := make(map[string]interface{})
	strClientID := ""
	strUsername := ""
	strPassword := ""
	strRemoteIP := ""
	boolIsActive := false

	//isStatus := false
	status := "400"
	statusDesc := "Failed"

	query := `SELECT client_id, api_username, api_password, api_remote_ip_address, is_active FROM user_api WHERE api_username = $1 AND client_id = $2;`

	rows, err := db.Query(query, incUsername, incClientID)

	if err == nil {
		// Success
		for rows.Next() {

			var rawClientID sql.NullString
			var rawUsername sql.NullString
			var rawPassword sql.NullString
			var rawRemoteIP sql.NullString
			var rawIsActive sql.NullBool

			errS := rows.Scan(&rawClientID, &rawUsername, &rawPassword, &rawRemoteIP, &rawIsActive)

			if errS == nil {
				strClientID = modules.ConvertSQLNullStringToString(rawClientID)
				strUsername = modules.ConvertSQLNullStringToString(rawUsername)
				strPassword = modules.ConvertSQLNullStringToString(rawPassword)
				strRemoteIP = modules.ConvertSQLNullStringToString(rawRemoteIP)
				boolIsActive = modules.ConvertSQLNullBoolToBool(rawIsActive)

				if boolIsActive {
					if strings.Contains(strRemoteIP, incRemoteIPAddress) || strings.Contains(strRemoteIP, "ALL") {
						if strPassword == incPassword {
							accessToken := modules.GenerateUUID()
							refreshToken := modules.GenerateUUID()

							redisKeyAccess := Config.ConstRedisAPIAccessToken + strClientID + strUsername
							redisKeyRefresh := Config.ConstRedisAPIRefreshToken + strClientID + strUsername

							// - Set redis value
							mapAccessToken := make(map[string]interface{})
							mapAccessToken["clientid"] = strClientID
							mapAccessToken["username"] = strUsername
							mapAccessToken["remoteip"] = strRemoteIP
							mapAccessToken["accesstoken"] = accessToken
							mapResponse["accesstoken"] = accessToken

							redisValAccessToken := modules.ConvertMapInterfaceToJSON(mapAccessToken)
							fmt.Println(redisKeyAccess)
							errA := modules.RedisSet(redisClient, goContext, redisKeyAccess, redisValAccessToken, 1*time.Hour)
							if errA == nil {
								//isStatus = true

								// - Set redis value
								fmt.Println("Success save Access: ", strClientID+strUsername)
								mapRefreshToken := make(map[string]interface{})
								mapRefreshToken["clientid"] = strClientID
								mapRefreshToken["username"] = strUsername
								mapRefreshToken["remoteip"] = strRemoteIP
								mapRefreshToken["refreshtoken"] = refreshToken
								mapResponse["refreshtoken"] = refreshToken

								redisValRefreshToken := modules.ConvertMapInterfaceToJSON(mapRefreshToken)
								fmt.Println(redisKeyRefresh)
								errR := modules.RedisSet(redisClient, goContext, redisKeyRefresh, redisValRefreshToken, 1*time.Hour)
								if errR == nil {
									fmt.Println("Success save Refresh: ", strClientID+strUsername)
									//isStatus = true
									status = "000"
									statusDesc = "Success"
								} else {
									fmt.Println("Failed save Access: ", strClientID+strUsername)
									//isStatus = false
									status = "400"
									statusDesc = "Failed - Create refresh token"
								}
							} else {
								//isStatus = false
								fmt.Println("Failed save Access: ", strClientID+strUsername)
								status = "400"
								statusDesc = "Failed - Create access token"
							}
						} else {
							modules.DoLog("INFO", "", "APICredential", "createNewToken",
								"Failed User's password is unmatched. Error occur.", false, nil)
							//return status and desc is failed password
							status = "400"
							statusDesc = "Failed - User's password is unmatched"
						}
					} else {
						modules.DoLog("INFO", "", "APICredential", "createNewToken",
							"Failed User's IP not whitelisted. Error occur.", false, nil)
						//return status and desc is failed password
						status = "400"
						statusDesc = "Failed - User's IP not whitelisted"
					}
				} else {
					modules.DoLog("INFO", "", "APICredential", "createNewToken",
						"Failed User is inactive. Error occur.", false, nil)
					//return status and desc is inactive
					status = "400"
					statusDesc = "Failed - User is inactive"
				}
			} else {
				modules.DoLog("ERROR", "", "APICredential", "createNewToken",
					"Failed to read database. Error occur.", true, errS)
				status = "400"
				statusDesc = "Failed - Read database"
			}
		}
	} else {
		// Database Error
		modules.DoLog("ERROR", "", "APICredential", "createNewToken",
			"Failed to read database. Error occur.", true, err)
		status = "400"
		statusDesc = "Failed - Read database or not Found"
	}

	return mapResponse, status, statusDesc
}

func updateNewToken(redisClient *redis.Client, goContext context.Context, incClientID string,
	incUsername string, incRefreshToken string, incRemoteIPAddress string) (map[string]interface{}, string, string) {

	mapResponse := make(map[string]interface{})
	strClientID := ""
	strUsername := ""
	strRefreshToken := ""
	strRemoteIP := ""
	//boolIsActive := false

	//isStatus := false
	status := "400"
	statusDesc := "Failed"

	redisKey := Config.ConstRedisAPIRefreshToken + incClientID + incUsername
	redisVal, errG := modules.RedisGet(redisClient, goContext, redisKey)
	if errG == nil {
		//isRemoteIPValid := false
		mapRedisVal := modules.ConvertJSONStringToMap("", redisVal)
		strUsername = modules.GetStringFromMapInterface(mapRedisVal, "username")
		strClientID = modules.GetStringFromMapInterface(mapRedisVal, "clientid")
		strRefreshToken = modules.GetStringFromMapInterface(mapRedisVal, "refreshtoken")
		strRemoteIP = modules.GetStringFromMapInterface(mapRedisVal, "remoteip")
		if strings.Contains(strRemoteIP, incRemoteIPAddress) || strings.Contains(strRemoteIP, "ALL") {
			if strRefreshToken == incRefreshToken {
				accessToken := modules.GenerateUUID()
				refreshToken := modules.GenerateUUID()

				redisKeyAccess := Config.ConstRedisAPIAccessToken + strClientID + strUsername
				redisKeyRefresh := Config.ConstRedisAPIRefreshToken + strClientID + strUsername

				// - Set redis value
				mapAccessToken := make(map[string]interface{})
				mapAccessToken["clientid"] = strClientID
				mapAccessToken["username"] = strUsername
				mapAccessToken["remoteip"] = strRemoteIP
				mapAccessToken["accesstoken"] = accessToken
				mapResponse["accesstoken"] = accessToken

				redisValAccessToken := modules.ConvertMapInterfaceToJSON(mapAccessToken)
				fmt.Println(redisKeyAccess)
				errA := modules.RedisSet(redisClient, goContext, redisKeyAccess, redisValAccessToken, 1*time.Hour)
				if errA == nil {
					//isStatus = true

					// - Set redis value
					fmt.Println("Success save Access: ", strClientID+strUsername)
					mapRefreshToken := make(map[string]interface{})
					mapRefreshToken["clientid"] = strClientID
					mapRefreshToken["username"] = strUsername
					mapRefreshToken["remoteip"] = strRemoteIP
					mapRefreshToken["refreshtoken"] = refreshToken
					mapResponse["refreshtoken"] = refreshToken

					redisValRefreshToken := modules.ConvertMapInterfaceToJSON(mapRefreshToken)
					fmt.Println(redisKeyRefresh)
					errR := modules.RedisSet(redisClient, goContext, redisKeyRefresh, redisValRefreshToken, 1*time.Hour)
					if errR == nil {
						modules.DoLog("INFO", "", "APICredential", "updateNewToken",
							fmt.Sprintln("Success save Refresh: ", strClientID+strUsername)+". Error occur.", false, nil)
						//fmt.Println("Success save Refresh: ", strClientID+strUsername)
						//isStatus = true
						status = "000"
						statusDesc = "Success"
					} else {
						modules.DoLog("ERROR", "", "APICredential", "updateNewToken",
							fmt.Sprintln("Failed save Access: ", strClientID+strUsername)+". Error occur.", true, errR)
						//fmt.Println("Failed save Access: ", strClientID+strUsername)
						//isStatus = false
						status = "400"
						statusDesc = "Failed - Create refresh token"
					}
				} else {
					//isStatus = false
					modules.DoLog("ERROR", "", "APICredential", "updateNewToken",
						fmt.Sprintln("Failed save Access: ", strClientID+strUsername)+". Error occur.", true, errA)
					//fmt.Println("Failed save Access: ", strClientID+strUsername)
					status = "400"
					statusDesc = "Failed - Create access token"
				}
			} else {
				modules.DoLog("INFO", "", "APICredential", "updateNewToken",
					"Failed User's refresh is unmatched.", false, nil)
				//return status and desc is failed password
				status = "400"
				statusDesc = "Failed - User's refresh is unmatched or not found"
			}
		} else {
			modules.DoLog("INFO", "", "APICredential", "updateNewToken",
				"Failed User's IP not whitelisted.", false, nil)
			//return status and desc is failed password
			status = "400"
			statusDesc = "Failed - User's IP not whitelisted"
		}
	} else {
		modules.DoLog("ERROR", "", "APICredential", "updateNewToken",
			fmt.Sprintln("Failed get redis: ", strClientID+strUsername)+". Error occur.", true, errG)
		//fmt.Println("Failed get Redis: ", strClientID+strUsername)
		//isStatus = false
		status = "400"
		statusDesc = "Failed - Create refresh token"
	}

	return mapResponse, status, statusDesc
}

func deleteToken(redisClient *redis.Client, goContext context.Context, incClientID string,
	incUsername string) (map[string]interface{}, string, string) {

	mapResponse := make(map[string]interface{})
	//boolIsActive := false

	//isStatus := false
	status := "400"
	statusDesc := "Failed"

	redisKeyRefresh := Config.ConstRedisAPIRefreshToken + incClientID + incUsername
	redisKeyAccess := Config.ConstRedisAPIAccessToken + incClientID + incUsername
	//redisVal, errG := modules.RedisGet(redisClient, goContext, redisKey)
	errR := modules.RedisDel(redisClient, goContext, redisKeyRefresh)
	errA := modules.RedisDel(redisClient, goContext, redisKeyAccess)
	if errR == nil && errA == nil {
		//isRemoteIPValid := false
		modules.DoLog("INFO", "", "APICredential", "deleteToken",
			fmt.Sprintln("Success delete redis: ", incClientID+incUsername)+".", true, nil)
		//fmt.Println("Failed get Redis: ", strClientID+strUsername)
		//isStatus = false
		status = "000"
		statusDesc = "Success - Delete Token"

	} else {
		modules.DoLog("ERROR", "", "APICredential", "deleteToken",
			fmt.Sprintln("Failed delete accesstoken redis: ", incClientID+incUsername)+". Error occur.", true, errR)
		modules.DoLog("ERROR", "", "APICredential", "deleteToken",
			fmt.Sprintln("Failed delete refreshtoken redis: ", incClientID+incUsername)+". Error occur.", true, errA)
		//fmt.Println("Failed get Redis: ", strClientID+strUsername)
		//isStatus = false
		status = "000"
		statusDesc = "Failed - Delete token or already deleted"
	}

	return mapResponse, status, statusDesc
}

func ProcessGetNewToken(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	mapResult := make(map[string]interface{})

	statusDesc := "Failed"
	respStatus := "900"
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "APICredential", "ProcessGetNewToken",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "APICredential", "ProcessGetNewToken",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 {
			mapResult, respStatus, statusDesc = createNewToken(db, rc, cx, incClientID, incUsername, incPassword, incRemoteIPAddress)
		} else {
			modules.DoLog("ERROR", incTraceCode, "APICredential", "ProcessGetNewToken",
				"Request not valid", false, nil)
			statusDesc = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, "APICredential", "ProcessGetNewToken",
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		statusDesc = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["data"] = mapResult
	mapResponse["description"] = statusDesc
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

func ProcessRefreshToken(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	mapResult := make(map[string]interface{})

	statusDesc := "Failed"
	respStatus := "900"
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "APICredential", "ProcessRefreshToken",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "APICredential", "ProcessRefreshToken",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incRefreshToken := modules.GetStringFromMapInterface(mapIncoming, "refreshtoken")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

		if len(incUsername) > 0 && len(incRefreshToken) > 0 && len(incClientID) > 0 {
			mapResult, respStatus, statusDesc = updateNewToken(rc, cx, incClientID, incUsername, incRefreshToken, incRemoteIPAddress)
		} else {
			modules.DoLog("ERROR", incTraceCode, "APICredential", "ProcessRefreshToken",
				"Request not valid", false, nil)
			statusDesc = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, "APICredential", "ProcessRefreshToken",
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		statusDesc = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["data"] = mapResult
	mapResponse["description"] = statusDesc
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

func ProcessDeleteToken(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	mapResult := make(map[string]interface{})

	statusDesc := "Failed"
	respStatus := "900"
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "APICredential", "ProcessDeleteToken",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "APICredential", "ProcessDeleteToken",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

		if len(incUsername) > 0 && len(incClientID) > 0 {
			mapResult, respStatus, statusDesc = deleteToken(rc, cx, incClientID, incUsername)
		} else {
			modules.DoLog("ERROR", incTraceCode, "APICredential", "ProcessDeleteToken",
				"Request not valid", false, nil)
			statusDesc = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, "APICredential", "ProcessDeleteToken",
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		statusDesc = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["data"] = mapResult
	mapResponse["description"] = statusDesc
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
