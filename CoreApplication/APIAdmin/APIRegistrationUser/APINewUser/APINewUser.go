package APINewUser

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	moduleName = "APINewUser"
)

func saveToDatabase(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (bool, string, string, string) {
	const functionName = "saveToDatabase"
	isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incRemoteIP := modules.GetStringFromMapInterface(mapIncoming, "remoteip")
	incUserAPIPassword := modules.GetStringFromMapInterface(mapIncoming, "userapipassword")

	incUsername, incPassword, incKey := modules.GenerateAPICredential(incClientID)
	if len(incUserAPIPassword) > 0 {
		incPassword = incUserAPIPassword
	}
	incAPIID := modules.GenerateFormulaID(incClientID, incUsername)
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := `INSERT INTO user_api (api_id, client_id, api_username, api_password, api_key, api_remote_ip_address, 
                      api_create_datetime, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`

	result, err := db.Exec(query, incAPIID, incClientID, incUsername,
		incPassword, incKey, incRemoteIP, incTimeNow, true)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"Failed to insert tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			isSuccess = true
			modules.DoLog("INFO", incTraceCode, moduleName, functionName,
				"Success to insert tables.", true, nil)
		} else {
			isSuccess = false
			modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
				"Failed to insert tables. Error occur.", true, err)
		}
	}
	return isSuccess, incUsername, incPassword, incKey
}

func NewAPIUserProcess(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "NewAPIUserProcess"
	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	result := make(map[string]interface{})

	respStatus := "900"
	statusDesc := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, moduleName, functionName,
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, moduleName, functionName,
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incAccessToken := modules.GetStringFromMapInterface(mapIncoming, "accesstoken")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		incRemoteIP := modules.GetStringFromMapInterface(mapIncoming, "remoteip")

		if len(incUsername) > 0 && len(incAccessToken) > 0 && len(incClientID) > 0 && len(incRemoteIP) > 0 {

			isCredentialValid, _ := modules.DoCheckRedisCredential(redisClient, contextX, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {

				isSuccess, strUsername, strPassword, _ := saveToDatabase(db, incTraceCode, mapIncoming)

				if isSuccess {
					respStatus = "000"
					result["username"] = strUsername
					result["password"] = strPassword
				} else {
					respStatus = "900"
				}
			} else {
				modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
					"Request not valid", false, nil)
				statusDesc = "Invalid Request - token is invalid"
				respStatus = "103"
			}
		} else {
			modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
				"Request not valid", false, nil)
			statusDesc = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		statusDesc = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["description"] = statusDesc
	mapResponse["data"] = result
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
