package APIEnum

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"time"
)

func getEnumFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (string, []map[string]interface{}) {
	var mapReturns []map[string]interface{}
	responseStatus := "400"

	//isSuccess := false

	query := `SELECT enum_id, name_schedule FROM cron_schedule;`

	rows, err := db.Query(query)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to select tables. Error occur.", true, err)
	} else {

		defer rows.Close()

		for rows.Next() {
			mapReturn := make(map[string]interface{})

			var enumId sql.NullString
			var nameSchedule sql.NullString

			errS := rows.Scan(&enumId, &nameSchedule)

			if errS != nil {
				modules.DoLog("INFO", "", "LandingGRPC", "Package",
					"Failed to read database. Error occur.", true, errS)
				responseStatus = "901"
			} else {
				responseStatus = "000"
				strEnumId := modules.ConvertSQLNullStringToString(enumId)
				strNameSchedule := modules.ConvertSQLNullStringToString(nameSchedule)

				mapReturn["enum_id"] = strEnumId
				mapReturn["name_schedule"] = strNameSchedule
			}
			mapReturns = append(mapReturns, mapReturn)
		}
	}
	return responseStatus, mapReturns
}

func ProcessGetAll(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	var results []map[string]interface{}

	respStatus := "900"
	statusDesc := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "API", "Auth",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incAccessToken := modules.GetStringFromMapInterface(mapIncoming, "accesstoken")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

		if len(incUsername) > 0 && len(incClientID) > 0 && len(incAccessToken) > 0 {

			isCredentialValid := modules.DoCheckRedisCredential(rc, cx, incClientID, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				respStatus, results = getEnumFromPostgres(db, incTraceCode, mapIncoming)
			} else {
				modules.DoLog("ERROR", incTraceCode, "API", "Auth",
					"Request not valid", false, nil)
				statusDesc = "Invalid Request - token is invalid"
				respStatus = "103"
			}
		} else {
			modules.DoLog("ERROR", incTraceCode, "API", "Auth",
				"Request not valid", false, nil)
			statusDesc = "Invalid Request - invalid body request"
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, "API", "Auth",
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		statusDesc = "Invalid Request - no body request"
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["description"] = statusDesc
	mapResponse["data"] = results
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
