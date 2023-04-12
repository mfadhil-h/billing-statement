package APIGetClient

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	moduleName = "APIGetClient"
)

func getAllClientFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (string, []map[string]interface{}) {
	const functionName = "getAllClientFromPostgres"
	var mapReturns []map[string]interface{}
	responseStatus := "400"

	//isSuccess := false

	query := `SELECT client_id, group_id, client_name, client_address, client_region, client_country, client_email, 
       client_phone, client_currency, pic_name, pic_email, pic_phone, client_type, client_create_datetime, 
       cient_update_datetime, is_active FROM client WHERE is_active = $1`

	rows, err := db.Query(query, true)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"Failed to insert tables. Error occur.", true, err)
	} else {

		defer rows.Close()

		for rows.Next() {
			mapReturn := make(map[string]interface{})

			var clientId sql.NullString
			var groupId sql.NullString
			var clientName sql.NullString
			var clientAddress sql.NullString
			var clientRegion sql.NullString
			var clientCountry sql.NullString
			var clientEmail sql.NullString
			var clientPhone sql.NullString
			var clientCurrency sql.NullString
			var picName sql.NullString
			var picEmail sql.NullString
			var picPhone sql.NullString
			var clientType sql.NullString
			var clientCreateDatetime sql.NullTime
			var clientUpdateDatetime sql.NullTime
			var isActive sql.NullBool

			errS := rows.Scan(&clientId, &groupId, &clientName, &clientAddress, &clientRegion, &clientCountry,
				&clientEmail, &clientPhone, &clientCurrency, &picName, &picEmail, &picPhone, &clientType,
				&clientCreateDatetime, &clientUpdateDatetime, &isActive)

			if errS != nil {
				modules.DoLog("INFO", incTraceCode, moduleName, functionName,
					"Failed to read database. Error occur.", true, errS)
				responseStatus = "901"
			} else {
				responseStatus = "000"
				strClientId := modules.ConvertSQLNullStringToString(clientId)
				strGroupId := modules.ConvertSQLNullStringToString(groupId)
				strClientName := modules.ConvertSQLNullStringToString(clientName)
				strClientAddress := modules.ConvertSQLNullStringToString(clientAddress)
				strClientRegion := modules.ConvertSQLNullStringToString(clientRegion)
				strClientCountry := modules.ConvertSQLNullStringToString(clientCountry)
				strClientEmail := modules.ConvertSQLNullStringToString(clientEmail)
				strClientPhone := modules.ConvertSQLNullStringToString(clientPhone)
				strClientCurrency := modules.ConvertSQLNullStringToString(clientCurrency)
				strPicName := modules.ConvertSQLNullStringToString(picName)
				strPicEmail := modules.ConvertSQLNullStringToString(picEmail)
				strPicPhone := modules.ConvertSQLNullStringToString(picPhone)
				strClientType := modules.ConvertSQLNullStringToString(clientType)
				timeClientCreateDatetime := modules.ConvertSQLNullTimeToTime(clientCreateDatetime)
				timeClientUpdateDatetime := modules.ConvertSQLNullTimeToTime(clientUpdateDatetime)
				boolIsActive := modules.ConvertSQLNullBoolToBool(isActive)

				mapReturn["client_id"] = strClientId
				mapReturn["group_id"] = strGroupId
				mapReturn["client_name"] = strClientName
				mapReturn["client_address"] = strClientAddress
				mapReturn["client_region"] = strClientRegion
				mapReturn["client_country"] = strClientCountry
				mapReturn["client_email"] = strClientEmail
				mapReturn["client_phone"] = strClientPhone
				mapReturn["client_currency"] = strClientCurrency
				mapReturn["pic_name"] = strPicName
				mapReturn["pic_email"] = strPicEmail
				mapReturn["pic_phone"] = strPicPhone
				mapReturn["client_type"] = strClientType
				mapReturn["client_create_datetime"] = timeClientCreateDatetime
				mapReturn["cient_update_datetime"] = timeClientUpdateDatetime
				mapReturn["is_active"] = boolIsActive
			}
			mapReturns = append(mapReturns, mapReturn)
		}
	}
	return responseStatus, mapReturns
}

func GetAllProcess(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "GetAllProcess"

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	var results []map[string]interface{}

	respStatus := "900"
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, moduleName, functionName,
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, moduleName, functionName,
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

		incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")

		if len(incUsername) > 0 && len(incPassword) > 0 {
			respStatus, results = getAllClientFromPostgres(db, incTraceCode, mapIncoming)
		} else {
			modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
				"Request not valid", false, nil)
			respStatus = "103"
		}
	} else {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"incomingMessage length == 0. INVALID REQUEST. trxStatus 206", false, nil)
		respStatus = "103"
	}

	responseHeader["Content-Type"] = "application/json"

	mapResponse["data"] = results
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

func getOneClientFromPostgres(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (string, map[string]interface{}) {
	const functionName = "getOneClientFromPostgres"
	mapReturn := make(map[string]interface{})
	responseStatus := "400"

	//isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

	query := `SELECT client_id, group_id, client_name, client_address, client_region, client_country, client_email, 
       client_phone, client_currency, pic_name, pic_email, pic_phone, client_type, client_create_datetime, 
       cient_update_datetime, is_active FROM client WHERE client_id = $1 AND is_active = $2 LIMIT 1`

	rows, err := db.Query(query, incClientID, true)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"Failed to insert tables. Error occur.", true, err)
	} else {

		defer rows.Close()

		for rows.Next() {

			var clientId sql.NullString
			var groupId sql.NullString
			var clientName sql.NullString
			var clientAddress sql.NullString
			var clientRegion sql.NullString
			var clientCountry sql.NullString
			var clientEmail sql.NullString
			var clientPhone sql.NullString
			var clientCurrency sql.NullString
			var picName sql.NullString
			var picEmail sql.NullString
			var picPhone sql.NullString
			var clientType sql.NullString
			var clientCreateDatetime sql.NullTime
			var clientUpdateDatetime sql.NullTime
			var isActive sql.NullBool

			errS := rows.Scan(&clientId, &groupId, &clientName, &clientAddress, &clientRegion, &clientCountry,
				&clientEmail, &clientPhone, &clientCurrency, &picName, &picEmail, &picPhone, &clientType,
				&clientCreateDatetime, &clientUpdateDatetime, &isActive)

			if errS != nil {
				modules.DoLog("INFO", incTraceCode, moduleName, functionName,
					"Failed to read database. Error occur.", true, errS)
				responseStatus = "901"
			} else {
				responseStatus = "000"
				strClientId := modules.ConvertSQLNullStringToString(clientId)
				strGroupId := modules.ConvertSQLNullStringToString(groupId)
				strClientName := modules.ConvertSQLNullStringToString(clientName)
				strClientAddress := modules.ConvertSQLNullStringToString(clientAddress)
				strClientRegion := modules.ConvertSQLNullStringToString(clientRegion)
				strClientCountry := modules.ConvertSQLNullStringToString(clientCountry)
				strClientEmail := modules.ConvertSQLNullStringToString(clientEmail)
				strClientPhone := modules.ConvertSQLNullStringToString(clientPhone)
				strClientCurrency := modules.ConvertSQLNullStringToString(clientCurrency)
				strPicName := modules.ConvertSQLNullStringToString(picName)
				strPicEmail := modules.ConvertSQLNullStringToString(picEmail)
				strPicPhone := modules.ConvertSQLNullStringToString(picPhone)
				strClientType := modules.ConvertSQLNullStringToString(clientType)
				timeClientCreateDatetime := modules.ConvertSQLNullTimeToTime(clientCreateDatetime)
				timeClientUpdateDatetime := modules.ConvertSQLNullTimeToTime(clientUpdateDatetime)
				boolIsActive := modules.ConvertSQLNullBoolToBool(isActive)

				mapReturn["client_id"] = strClientId
				mapReturn["group_id"] = strGroupId
				mapReturn["client_name"] = strClientName
				mapReturn["client_address"] = strClientAddress
				mapReturn["client_region"] = strClientRegion
				mapReturn["client_country"] = strClientCountry
				mapReturn["client_email"] = strClientEmail
				mapReturn["client_phone"] = strClientPhone
				mapReturn["client_currency"] = strClientCurrency
				mapReturn["pic_name"] = strPicName
				mapReturn["pic_email"] = strPicEmail
				mapReturn["pic_phone"] = strPicPhone
				mapReturn["client_type"] = strClientType
				mapReturn["client_create_datetime"] = timeClientCreateDatetime
				mapReturn["cient_update_datetime"] = timeClientUpdateDatetime
				mapReturn["is_active"] = boolIsActive
			}
		}
	}
	return responseStatus, mapReturn
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
		incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

		if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 {

			respStatus, results = getOneClientFromPostgres(db, incTraceCode, mapIncoming)
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
