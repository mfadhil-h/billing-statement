package APINewUser

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

func saveToDatabase(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (bool, string, string, string) {

	isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incRemoteIP := modules.GetStringFromMapInterface(mapIncoming, "remoteip")

	incUsername, incPassword, incKey := modules.GenerateAPICredential(incClientID)
	incAPIID := modules.GenerateFormulaID(incClientID, incUsername)
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "INSERT INTO user_api (api_id, client_id, api_username, " +
		"api_password, api_key, api_remote_ip_address, api_create_datetime, is_active) " +
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"

	result, err := db.Exec(query, incAPIID, incClientID, incUsername,
		incPassword, incKey, incRemoteIP, incTimeNow, true)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to insert tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			isSuccess = true
			modules.DoLog("INFO", incTraceCode, "API", "Formula",
				"Success to insert tables.", true, nil)
		} else {
			isSuccess = false
			modules.DoLog("ERROR", incTraceCode, "API", "Formula",
				"Failed to insert tables. Error occur.", true, err)
		}
	}
	return isSuccess, incUsername, incPassword, incKey
}

func Process(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	respStatus := "900"
	respUsername := ""
	respPassword := ""
	respKey := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "API", "Auth",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		incRemoteIP := modules.GetStringFromMapInterface(mapIncoming, "remoteip")

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incRemoteIP) > 0 {

			isSuccess, strUsername, strPassword, strKey := saveToDatabase(db, incTraceCode, mapIncoming)

			if isSuccess {
				respStatus = "000"
				respUsername = strUsername
				respPassword = strPassword
				respKey = strKey
			} else {
				respStatus = "900"
			}
		} else {
			modules.DoLog("ERROR", incTraceCode, "API", "Auth",
				"Request not valid", false, nil)
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, "API", "Auth",
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["username"] = respUsername
	mapResponse["password"] = respPassword
	mapResponse["key"] = respKey
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
