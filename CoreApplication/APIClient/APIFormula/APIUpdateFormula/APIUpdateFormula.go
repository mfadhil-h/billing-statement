package APIUpdateFormula

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

	incField1 := modules.GetStringFromMapInterface(mapIncoming, "par1")
	incField2 := modules.GetStringFromMapInterface(mapIncoming, "par2")
	incField3 := modules.GetStringFromMapInterface(mapIncoming, "par3")
	incField4 := modules.GetStringFromMapInterface(mapIncoming, "par4")
	incField5 := modules.GetStringFromMapInterface(mapIncoming, "par5")
	incField6 := modules.GetStringFromMapInterface(mapIncoming, "par6")
	incField7 := modules.GetStringFromMapInterface(mapIncoming, "par7")
	incField8 := modules.GetStringFromMapInterface(mapIncoming, "par8")
	incField9 := modules.GetStringFromMapInterface(mapIncoming, "par9")
	incField10 := modules.GetStringFromMapInterface(mapIncoming, "par10")

	incField11 := modules.GetStringFromMapInterface(mapIncoming, "par11")
	incField12 := modules.GetStringFromMapInterface(mapIncoming, "par12")
	incField13 := modules.GetStringFromMapInterface(mapIncoming, "par13")
	incField14 := modules.GetStringFromMapInterface(mapIncoming, "par14")
	incField15 := modules.GetStringFromMapInterface(mapIncoming, "par15")
	incField16 := modules.GetStringFromMapInterface(mapIncoming, "par16")
	incField17 := modules.GetStringFromMapInterface(mapIncoming, "par17")
	incField18 := modules.GetStringFromMapInterface(mapIncoming, "par18")
	incField19 := modules.GetStringFromMapInterface(mapIncoming, "par19")
	incField20 := modules.GetStringFromMapInterface(mapIncoming, "par20")

	incFormula := modules.GetStringFromMapInterface(mapIncoming, "formula")
	incFormulaName := modules.GetStringFromMapInterface(mapIncoming, "name")
	incType := modules.GetStringFromMapInterface(mapIncoming, "type")
	incTime := modules.GetStringFromMapInterface(mapIncoming, "time")

	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "UPDATE formula " +
		"SET formula_name = $3, " +
		"field1 = $4, field2 = $5, field3 = $6, field4 = $7, field5 = $8, field6 = $9, field7 = $10, field8 = $11, " +
		"field9 = $12, field10 = $13, field11 = $14, field12 = $15, field13 = $16, field14 = $17, field15 = $18, " +
		"field16 = $19, field17 = $20, field18 = $21, field19 = $22, field20 = $23, " +
		"formula = $24, formula_type = $25, formula_time = $26, formula_update_datetime = $27, is_active = $28 " +
		"WHERE client_id = $1 AND formula_id = $2"

	result, err := db.Exec(query, incClientID, incFormulaID, incFormulaName,
		incField1, incField2, incField3, incField4, incField5, incField6, incField7, incField8, incField9, incField10,
		incField11, incField12, incField13, incField14, incField15, incField16, incField17, incField18, incField19, incField20,
		incFormula, incType, incTime, incTimeNow, true)

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
	return isSuccess
}


func Process(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	respStatus := "900"
	respDescription := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "API", "Auth",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0  {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incFormulaID) > 0 {

			isSuccess := saveToDatabase(db, incTraceCode, mapIncoming)

			if isSuccess {
				respDescription = "Formula successfully updated and will be running in the next 1 hour"
				respStatus = "000"
			} else {
				respDescription = "Formula failed to update"
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

	mapResponse["description"] = respDescription
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

