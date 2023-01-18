package APISendData

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

func saveToDatabase(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) bool {

	isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

	incField1 := modules.GetFloatFromMapInterface(mapIncoming, "data1")
	incField2 := modules.GetFloatFromMapInterface(mapIncoming, "data2")
	incField3 := modules.GetFloatFromMapInterface(mapIncoming, "data3")
	incField4 := modules.GetFloatFromMapInterface(mapIncoming, "data4")
	incField5 := modules.GetFloatFromMapInterface(mapIncoming, "data5")
	incField6 := modules.GetFloatFromMapInterface(mapIncoming, "data6")
	incField7 := modules.GetFloatFromMapInterface(mapIncoming, "data7")
	incField8 := modules.GetFloatFromMapInterface(mapIncoming, "data8")
	incField9 := modules.GetFloatFromMapInterface(mapIncoming, "data9")
	incField10 := modules.GetFloatFromMapInterface(mapIncoming, "data10")

	incField11 := modules.GetFloatFromMapInterface(mapIncoming, "data11")
	incField12 := modules.GetFloatFromMapInterface(mapIncoming, "data12")
	incField13 := modules.GetFloatFromMapInterface(mapIncoming, "data13")
	incField14 := modules.GetFloatFromMapInterface(mapIncoming, "data14")
	incField15 := modules.GetFloatFromMapInterface(mapIncoming, "data15")
	incField16 := modules.GetFloatFromMapInterface(mapIncoming, "data16")
	incField17 := modules.GetFloatFromMapInterface(mapIncoming, "data17")
	incField18 := modules.GetFloatFromMapInterface(mapIncoming, "data18")
	incField19 := modules.GetFloatFromMapInterface(mapIncoming, "data19")
	incField20 := modules.GetFloatFromMapInterface(mapIncoming, "data20")

	incDataID := modules.GenerateUUID()
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "INSERT INTO client_data (data_id, client_id, formula_id, " +
		"field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, " +
		"field11, field12, field13, field14, field15, field16, field17, field18, field19, field20," +
		"data_receive_datetime, data_receive_code, is_process) " +
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)"

	result, err := db.Exec(query, incDataID, incClientID, incFormulaID,
		incField1, incField2, incField3, incField4, incField5, incField6, incField7, incField8, incField9, incField10,
		incField11, incField12, incField13, incField14, incField15, incField16, incField17, incField18, incField19, incField20,
		incTimeNow, incTraceCode, false)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "API", "Receiver",
			"Failed to insert tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			isSuccess = true
			modules.DoLog("INFO", incTraceCode, "API", "Receiver",
				"Success to insert tables.", true, nil)
		} else {
			isSuccess = false
			modules.DoLog("ERROR", incTraceCode, "API", "Receiver",
				"Failed to insert tables. Error occur.", true, err)
		}
	}
	return isSuccess
}


func Process(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	respStatus := "900"
	respUniqueCode := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "API", "Auth",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0  {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incKey := modules.GetStringFromMapInterface(mapIncoming, "key")
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

		isCredentialValid := modules.DoCheckRedisClientHit(rc, cx, incClientID, incUsername, incPassword, incKey, incRemoteIPAddress)
		isValid := modules.DoCheckFormulaID(rc, cx, incFormulaID)

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incFormulaID) > 0 && isValid && isCredentialValid {

			isSuccess := saveToDatabase(db, incTraceCode, mapIncoming)

			if isSuccess {
				respStatus = "000"
				respUniqueCode = incTraceCode
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

	mapResponse["receivecode"] = respUniqueCode
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

