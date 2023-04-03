package SendData

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

func Process(dbPostgres *sql.DB, dbMongo *mongo.Database, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	respStatus := "900"
	respUniqueCode := ""
	respDescription := ""
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
		incAccessToken := modules.GetStringFromMapInterface(mapIncoming, "accesstoken")
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

		isValid := modules.DoCheckFormulaID(rc, cx, incFormulaID)

		if len(incUsername) > 0 && len(incClientID) > 0 && len(incFormulaID) > 0 && isValid && len(incAccessToken) > 0 {

			isCredentialValid := modules.DoCheckRedisCredential(rc, cx, incClientID, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {

				isSuccess := modules.SaveDataBillingIntoMongo(dbMongo, rc, cx, incTraceCode, mapIncoming)

				if isSuccess {
					respStatus = "000"
					respDescription = "Success"
					respUniqueCode = incTraceCode
				} else {
					respDescription = "Failed"
					respStatus = "900"
				}
			} else {
				modules.DoLog("ERROR", incTraceCode, "API", "Auth",
					"Request not valid", false, nil)
				respDescription = "Invalid Request - token is invalid"
				respStatus = "103"
			}
		} else {
			modules.DoLog("ERROR", incTraceCode, "API", "Auth",
				"Request not valid", false, nil)
			respDescription = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, "API", "Auth",
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		respDescription = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["receivecode"] = respUniqueCode
	mapResponse["description"] = respDescription
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
