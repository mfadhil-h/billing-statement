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

func Process(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	isTypeValid := false
	respStatus := "900"
	respFormulaID := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "API", "Auth",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)
		/* TEMP REMOVE AUTH FOR TEST */

		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incKey := modules.GetStringFromMapInterface(mapIncoming, "key")
		incType := modules.GetStringFromMapInterface(mapIncoming, "type")
		incTime := modules.GetStringFromMapInterface(mapIncoming, "time")

		isCredentialValid := modules.DoCheckRedisClientHit(rc, cx, incClientID, incUsername, incPassword, incKey, incRemoteIPAddress)

		if strings.ToUpper(incType) != "REALTIME" && len(incTime) == 0 {
			isTypeValid = false
		} else {
			isTypeValid = true
		}

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incType) > 0 && isTypeValid && isCredentialValid {

			//isSuccess, strFormulaID := saveToDatabase(db, incTraceCode, mapIncoming)
			//isSuccess, strFormulaID := modules.SaveFormulaBillingIntoPg(db, incTraceCode, mapIncoming)
			isSuccess, strFormulaID := modules.SaveFormulaArrayBillingIntoPg(db, incTraceCode, mapIncoming)

			if isSuccess {
				modules.ReloadFormulaToRedis(db, rc, cx, strFormulaID)

				respStatus = "000"
				respFormulaID = strFormulaID
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

	mapResponse["formulaid"] = respFormulaID
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
