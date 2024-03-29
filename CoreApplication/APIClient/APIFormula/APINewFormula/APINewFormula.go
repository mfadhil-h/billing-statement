package APINewFormula

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
	moduleName = "APINewFormula"
)

func NewFormulaProcess(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "NewFormulaProcess"

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	isTypeValid := false
	respStatus := "900"
	statusDesc := ""
	respFormulaID := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, moduleName, functionName,
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, moduleName, functionName,
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)
		/* TEMP REMOVE AUTH FOR TEST */

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incAccessToken := modules.GetStringFromMapInterface(mapIncoming, "accesstoken")
		incType := modules.GetStringFromMapInterface(mapIncoming, "type")
		incTime := modules.GetStringFromMapInterface(mapIncoming, "time")

		if strings.ToUpper(incType) != "REALTIME" && len(incTime) == 0 {
			isTypeValid = false
		} else {
			isTypeValid = true
		}

		if len(incUsername) > 0 && len(incType) > 0 && isTypeValid && len(incAccessToken) > 0 {

			isCredentialValid, incClientID := modules.DoCheckRedisCredential(rc, cx, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				//isSuccess, strFormulaID := saveToDatabase(db, incTraceCode, mapIncoming)
				//isSuccess, strFormulaID := modules.SaveFormulaBillingIntoPg(db, incTraceCode, mapIncoming)
				isSuccess := false
				strFormulaID := ""
				isSuccess, strFormulaID, statusDesc = modules.SaveFormulaArrayBillingIntoPg(db, incTraceCode, mapIncoming, incClientID)

				if isSuccess {
					modules.ReloadFormulaToRedis(db, rc, cx, strFormulaID)

					respStatus = "000"
					respFormulaID = strFormulaID
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

	mapResponse["formulaid"] = respFormulaID
	mapResponse["status"] = respStatus
	mapResponse["description"] = statusDesc
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
