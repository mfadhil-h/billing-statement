package APIGetFormula

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lib/pq"
	"time"
)

func getAllFormulaClientFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (string, []map[string]interface{}) {
	var mapReturns []map[string]interface{}
	responseStatus := "400"

	//isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

	query := `SELECT formula_id, client_id, formula_name, fields, formula, formula_type, formula_time, 
        formula_create_datetime, formula_update_datetime, is_active FROM yformula_v3 
        WHERE client_id = $1`

	rows, err := db.Query(query, incClientID)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to insert tables. Error occur.", true, err)
	} else {

		defer rows.Close()

		for rows.Next() {
			mapReturn := make(map[string]interface{})

			var formulaId sql.NullString
			var clientId sql.NullString
			var formulaName sql.NullString
			var fields []string
			var formula sql.NullString
			var formulaType sql.NullString
			var formulaTime sql.NullTime
			var formulaCreateDatetime sql.NullTime
			var formulaUpdateDatetime sql.NullTime
			var isActive sql.NullBool

			errS := rows.Scan(&formulaId, &clientId, &formulaName, (*pq.StringArray)(&fields), &formula, &formulaType,
				&formulaTime, &formulaCreateDatetime, &formulaUpdateDatetime, &isActive)

			if errS != nil {
				modules.DoLog("INFO", "", "LandingGRPC", "Package",
					"Failed to read database. Error occur.", true, errS)
				responseStatus = "901"
			} else {
				responseStatus = "000"
				strClientId := modules.ConvertSQLNullStringToString(clientId)
				strFormulaId := modules.ConvertSQLNullStringToString(formulaId)
				strFormulaName := modules.ConvertSQLNullStringToString(formulaName)
				strFormula := modules.ConvertSQLNullStringToString(formula)
				strFormulaType := modules.ConvertSQLNullStringToString(formulaType)
				strFormulaTime := modules.DoFormatDateTime("HH:mm:ss", modules.ConvertSQLNullTimeToTime(formulaTime))
				timeFormulaCreateDatetime := modules.ConvertSQLNullTimeToTime(formulaCreateDatetime)
				timeFormulaUpdateDatetime := modules.ConvertSQLNullTimeToTime(formulaUpdateDatetime)
				boolIsActive := modules.ConvertSQLNullBoolToBool(isActive)

				mapReturn["client_id"] = strClientId
				mapReturn["formula_id"] = strFormulaId
				mapReturn["formula_name"] = strFormulaName
				mapReturn["fields"] = fields
				mapReturn["formula"] = strFormula
				mapReturn["formula_type"] = strFormulaType
				mapReturn["formula_time"] = strFormulaTime
				mapReturn["formula_create_datetime"] = timeFormulaCreateDatetime
				mapReturn["formula_update_datetime"] = timeFormulaUpdateDatetime
				mapReturn["is_active"] = boolIsActive
			}
			mapReturns = append(mapReturns, mapReturn)
		}
	}
	return responseStatus, mapReturns
}

func ProcessGetAll(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	var results []map[string]interface{}

	respStatus := "900"
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

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 {
			respStatus, results = getAllFormulaClientFromPostgres(db, incTraceCode, mapIncoming)
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

	mapResponse["data"] = results
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

func getOneFormulaFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (string, map[string]interface{}) {
	mapReturn := make(map[string]interface{})
	responseStatus := "400"

	//isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

	query := `SELECT formula_id, client_id, formula_name, fields, formula, formula_type, formula_time, 
        formula_create_datetime, formula_update_datetime, is_active FROM yformula_v3 
        WHERE formula_id = $1 AND client_id = $2 LIMIT 1`

	rows, err := db.Query(query, incFormulaID, incClientID)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to insert tables. Error occur.", true, err)
	} else {

		defer rows.Close()

		for rows.Next() {

			var formulaId sql.NullString
			var clientId sql.NullString
			var formulaName sql.NullString
			var fields []string
			var formula sql.NullString
			var formulaType sql.NullString
			var formulaTime sql.NullTime
			var formulaCreateDatetime sql.NullTime
			var formulaUpdateDatetime sql.NullTime
			var isActive sql.NullBool

			errS := rows.Scan(&formulaId, &clientId, &formulaName, (*pq.StringArray)(&fields), &formula, &formulaType,
				&formulaTime, &formulaCreateDatetime, &formulaUpdateDatetime, &isActive)

			if errS != nil {
				modules.DoLog("INFO", "", "LandingGRPC", "Package",
					"Failed to read database. Error occur.", true, errS)
				responseStatus = "901"
			} else {
				responseStatus = "000"
				strClientId := modules.ConvertSQLNullStringToString(clientId)
				strFormulaId := modules.ConvertSQLNullStringToString(formulaId)
				strFormulaName := modules.ConvertSQLNullStringToString(formulaName)
				strFormula := modules.ConvertSQLNullStringToString(formula)
				strFormulaType := modules.ConvertSQLNullStringToString(formulaType)
				strFormulaTime := modules.DoFormatDateTime("HH:mm:ss", modules.ConvertSQLNullTimeToTime(formulaTime))
				timeFormulaCreateDatetime := modules.ConvertSQLNullTimeToTime(formulaCreateDatetime)
				timeFormulaUpdateDatetime := modules.ConvertSQLNullTimeToTime(formulaUpdateDatetime)
				boolIsActive := modules.ConvertSQLNullBoolToBool(isActive)

				mapReturn["client_id"] = strClientId
				mapReturn["formula_id"] = strFormulaId
				mapReturn["formula_name"] = strFormulaName
				mapReturn["fields"] = fields
				mapReturn["formula"] = strFormula
				mapReturn["formula_type"] = strFormulaType
				mapReturn["formula_time"] = strFormulaTime
				mapReturn["formula_create_datetime"] = timeFormulaCreateDatetime
				mapReturn["formula_update_datetime"] = timeFormulaUpdateDatetime
				mapReturn["is_active"] = boolIsActive
			}
		}
	}
	return responseStatus, mapReturn
}

func ProcessGetById(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	results := make(map[string]interface{})

	respStatus := "900"
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
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incFormulaID) > 0 {

			respStatus, results = getOneFormulaFromPostgres(db, incTraceCode, mapIncoming)
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

	mapResponse["data"] = results
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
