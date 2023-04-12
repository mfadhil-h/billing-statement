package APIActivateFormula

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
)

const (
	moduleName = "APIActivateFormula"
)

func saveToDatabase(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, mapIncoming map[string]interface{}, incClientID string) bool {
	const functionName = "saveToDatabase"
	isSuccess := false
	isActive := false

	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
	incStatus := modules.GetStringFromMapInterface(mapIncoming, "reqtype")

	if strings.ToUpper(incStatus) == "ACTIVE" {
		modules.ReloadFormulaToRedis(db, rc, cx, incFormulaID)
		isActive = true
	} else if strings.ToUpper(incStatus) == "DEACTIVE" {
		modules.RemoveFormulaFromRedis(db, rc, cx, incFormulaID)
		isActive = false
	}

	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "UPDATE formula " +
		"SET formula_update_datetime = $3, is_active = $4 " +
		"WHERE client_id = $1 AND formula_id = $2"

	result, err := db.Exec(query, incClientID, incFormulaID, incTimeNow, isActive)

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
	return isSuccess
}

func ActivateProcess(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "ActivateProcess"
	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	respStatus := "900"
	respDescription := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, moduleName, functionName,
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, moduleName, functionName,
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incAccessToken := modules.GetStringFromMapInterface(mapIncoming, "accesstoken")
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
		incStatus := modules.GetStringFromMapInterface(mapIncoming, "reqtype")

		if len(incUsername) > 0 && len(incFormulaID) > 0 && len(incStatus) > 0 && len(incAccessToken) > 0 {

			isCredentialValid, incClientID := modules.DoCheckRedisCredential(rc, cx, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				isSuccess := saveToDatabase(db, rc, cx, incTraceCode, mapIncoming, incClientID)

				if isSuccess {
					respStatus = "000"
					if strings.ToUpper(incStatus) == "ACTIVE" {
						respDescription = "Formula successfully activated"
					} else if strings.ToUpper(incStatus) == "DEACTIVE" {
						respDescription = "Formula successfully deactivated"
					}
				} else {
					respStatus = "900"
				}
			} else {
				modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
					"Request not valid", false, nil)
				respDescription = "Invalid Request - token is invalid"
				respStatus = "103"
			}
		} else {
			modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
				"Request not valid", false, nil)
			respDescription = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		respDescription = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["descripton"] = respDescription
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
