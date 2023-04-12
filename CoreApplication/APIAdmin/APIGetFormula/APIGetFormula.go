package APIGetFormula

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lib/pq"
	"strings"
	"time"
)

const (
	moduleName = "APIGetFormula"
)

func getAllFormulaClientFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}, incClientID string) (string, string, []map[string]interface{}) {
	const functionName = "getAllFormulaClientFromPostgres"
	var mapReturns []map[string]interface{}
	responseStatus := "400"
	responseDesc := "Error - Empty"

	//isSuccess := false

	query := `SELECT formula_id, client_id, formula_name, fields, formula, formula_type, formula_time, 
        formula_create_datetime, formula_update_datetime, is_active FROM yformula_v3 
        WHERE client_id = $1`

	rows, err := db.Query(query, incClientID)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"Failed to insert tables. Error occur.", true, err)
		responseStatus = "901"
	} else {
		responseStatus = "000"
		responseDesc = "Empty Data"

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
				modules.DoLog("INFO", incTraceCode, moduleName, functionName,
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

				var strInsertAsFormula []string

				var strInsertAsResultString []string

				var strOutputHeader []string

				var strOutputDataGroup []string
				var strOutputRecapGroup []string

				strFormula = strings.ReplaceAll(strFormula, "@", "")
				arrFormulas := strings.Split(strFormula, "\n")
				for x := 0; x < len(arrFormulas); x++ {
					arrContent := strings.Split(arrFormulas[x], ":")

					if len(arrFormulas[x]) > 1 && len(arrContent) > 1 {
						incID := arrContent[0]
						incParameter := strings.TrimLeft(strings.TrimRight(arrContent[1], " "), " ")
						modules.DoLog("INFO", incTraceCode, moduleName, functionName,
							"getOneFormula: "+fmt.Sprintf("incID: %+v", incID)+fmt.Sprintf(", incParameter: %+v", incParameter), false, nil)

						if strings.ToUpper(incID) == "STRING" {
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strInsertAsResultString = append(strInsertAsResultString, rawResults)
								}
							}

						} else if strings.ToUpper(incID) == "FORMULA" {
							strInsertAsFormula = append(strInsertAsFormula, incParameter)
						} else if strings.ToUpper(incID) == "OUTPUTHEADER" {
							strOutputHeader = append(strOutputHeader, incParameter)
						} else if strings.ToUpper(incID) == "OUTPUTDATAGROUP" {
							strOutputDataGroup = append(strOutputDataGroup, incParameter)
						} else if strings.ToUpper(incID) == "OUTPUTRECAPGROUP" {
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strOutputRecapGroup = append(strOutputRecapGroup, rawFormulas)
								}
							}
						}
					}
				}

				var mapFormula = make(map[string]interface{})
				mapFormula["formula"] = strInsertAsFormula
				mapFormula["string"] = strInsertAsResultString
				mapFormula["outputheader"] = strOutputHeader
				mapFormula["outputdatagroup"] = strOutputDataGroup
				mapFormula["outputrecapgroup"] = strOutputRecapGroup

				mapReturn["client_id"] = strClientId
				mapReturn["formula_id"] = strFormulaId
				mapReturn["formula_name"] = strFormulaName
				mapReturn["fields"] = fields
				mapReturn["formula"] = mapFormula
				mapReturn["formula_type"] = strFormulaType
				mapReturn["formula_time"] = strFormulaTime
				mapReturn["formula_create_datetime"] = timeFormulaCreateDatetime
				mapReturn["formula_update_datetime"] = timeFormulaUpdateDatetime
				mapReturn["is_active"] = boolIsActive
			}
			mapReturns = append(mapReturns, mapReturn)
		}

		if len(mapReturns) > 0 {
			responseDesc = "Success"
		} else {
			mapReturns = make([]map[string]interface{}, 0)
		}
	}
	return responseDesc, responseStatus, mapReturns
}

func GetAllProcess(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "GetAllProcess"
	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	var results []map[string]interface{}

	respStatus := "900"
	statusDesc := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, moduleName, functionName,
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, moduleName, functionName,
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incAccessToken := modules.GetStringFromMapInterface(mapIncoming, "accesstoken")

		if len(incUsername) > 0 && len(incAccessToken) > 0 {

			isCredentialValid, incClientID := modules.DoCheckRedisCredential(rc, cx, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				statusDesc, respStatus, results = getAllFormulaClientFromPostgres(db, incTraceCode, mapIncoming, incClientID)
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

	mapResponse["description"] = statusDesc
	mapResponse["data"] = results
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

func GetOneFormulaFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}, incClientID string) (string, string, map[string]interface{}) {
	const functionName = "GetOneFormulaFromPostgres"
	mapReturn := make(map[string]interface{})
	responseStatus := "400"
	responseDesc := "Error - Empty"

	//isSuccess := false

	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

	query := `SELECT formula_id, client_id, formula_name, fields, formula, formula_type, formula_time, 
        formula_create_datetime, formula_update_datetime, is_active FROM yformula_v3 
        WHERE formula_id = $1 AND client_id = $2 LIMIT 1`

	rows, err := db.Query(query, incFormulaID, incClientID)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"Failed to insert tables. Error occur.", true, err)
		responseStatus = "901"
	} else {
		responseStatus = "000"
		responseDesc = "Empty Data"

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
				modules.DoLog("INFO", incTraceCode, moduleName, functionName,
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

				var strInsertAsFormula []string

				var strInsertAsResultString []string

				var strOutputHeader []string

				var strOutputDataGroup []string
				var strOutputRecapGroup []string

				strFormula = strings.ReplaceAll(strFormula, "@", "")
				arrFormulas := strings.Split(strFormula, "\n")
				for x := 0; x < len(arrFormulas); x++ {
					arrContent := strings.Split(arrFormulas[x], ":")

					if len(arrFormulas[x]) > 1 && len(arrContent) > 1 {
						incID := arrContent[0]
						incParameter := strings.TrimLeft(strings.TrimRight(arrContent[1], " "), " ")
						modules.DoLog("INFO", incTraceCode, moduleName, functionName,
							"getOneFormula: "+fmt.Sprintf("incID: %+v", incID)+fmt.Sprintf(", incParameter: %+v", incParameter), false, nil)

						if strings.ToUpper(incID) == "STRING" {
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {
								if y == 0 {
									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strInsertAsResultString = append(strInsertAsResultString, rawResults)
								}
							}

						} else if strings.ToUpper(incID) == "FORMULA" {
							strInsertAsFormula = append(strInsertAsFormula, incParameter)
						} else if strings.ToUpper(incID) == "OUTPUTHEADER" {
							strOutputHeader = append(strOutputHeader, incParameter)
						} else if strings.ToUpper(incID) == "OUTPUTDATAGROUP" {
							strOutputDataGroup = append(strOutputDataGroup, incParameter)
						} else if strings.ToUpper(incID) == "OUTPUTRECAPGROUP" {
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strOutputRecapGroup = append(strOutputRecapGroup, rawFormulas)
								}
							}
						}
					}
				}

				var mapFormula = make(map[string]interface{})
				mapFormula["formula"] = strInsertAsFormula
				mapFormula["string"] = strInsertAsResultString
				mapFormula["outputheader"] = strOutputHeader
				mapFormula["outputdatagroup"] = strOutputDataGroup
				mapFormula["outputrecapgroup"] = strOutputRecapGroup

				mapReturn["client_id"] = strClientId
				mapReturn["formula_id"] = strFormulaId
				mapReturn["formula_name"] = strFormulaName
				mapReturn["fields"] = fields
				mapReturn["formula"] = mapFormula
				mapReturn["formula_type"] = strFormulaType
				mapReturn["formula_time"] = strFormulaTime
				mapReturn["formula_create_datetime"] = timeFormulaCreateDatetime
				mapReturn["formula_update_datetime"] = timeFormulaUpdateDatetime
				mapReturn["is_active"] = boolIsActive
			}
		}

		if len(mapReturn) > 0 {
			responseDesc = "Success"
		} else {
			mapReturn = make(map[string]interface{})
		}
	}
	return responseDesc, responseStatus, mapReturn
}

func GetByIdProcess(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "GetByIdProcess"
	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	results := make(map[string]interface{})

	respStatus := "900"
	statusDesc := ""
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

			isCredentialValid, incClientID := modules.DoCheckRedisCredential(redisClient, contextX, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {

				statusDesc, respStatus, results = GetOneFormulaFromPostgres(db, incTraceCode, mapIncoming, incClientID)
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

	mapResponse["data"] = results
	mapResponse["description"] = statusDesc
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
