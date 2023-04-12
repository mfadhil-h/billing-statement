package APIUpdateFormula

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
	moduleName = "APIUpdateFormula"
)

func saveToDatabase(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}, incClientID string) bool {
	const functionName = "saveToDatabase"
	isSuccess := false

	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
	strFormula := ""
	incFields := mapIncoming["fields"].(interface{})

	incFormulaMap := mapIncoming["formula"].(map[string]interface{})
	for key, incItem := range incFormulaMap {
		arrItem := incItem.([]interface{})
		for _, item := range arrItem {
			strFormula += "@" + key + ": " + item.(string) + "@\n"
		}
	}
	if strings.HasSuffix(strFormula, "\n") {
		strings.TrimSuffix(strFormula, "\n")
	}

	//incFormula := modules.GetStringFromMapInterface(mapIncoming, "formula")
	incFormulaName := modules.GetStringFromMapInterface(mapIncoming, "name")
	incType := modules.GetStringFromMapInterface(mapIncoming, "type")
	incTime := modules.GetStringFromMapInterface(mapIncoming, "time")

	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := `UPDATE yformula_v3 
		SET formula_name = $3,  
		fields = $4,
		formula = $5, formula_type = $6, formula_time = $7, formula_update_datetime = $8, is_active = $9  
		WHERE client_id = $1 AND formula_id = $2`

	result, err := db.Exec(query, incClientID, incFormulaID, incFormulaName,
		incFields, strFormula, incType, incTime, incTimeNow, true)

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

func UpdateProcess(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "UpdateProcess"
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

		if len(incUsername) > 0 && len(incFormulaID) > 0 && len(incAccessToken) > 0 {

			isCredentialValid, incClientID := modules.DoCheckRedisCredential(rc, cx, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				isSuccess := saveToDatabase(db, incTraceCode, mapIncoming, incClientID)

				if isSuccess {
					respDescription = "Formula successfully updated and will be running in the next 1 hour"
					respStatus = "000"
				} else {
					respDescription = "Formula failed to update"
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

	mapResponse["description"] = respDescription
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
