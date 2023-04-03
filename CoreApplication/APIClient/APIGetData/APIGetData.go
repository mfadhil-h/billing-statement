package APIGetData

import (
	"billing/CoreApplication/APIClient/APIFormula/APIGetFormula"
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

func getAllFormulaDataFromMongo(dbMongo *mongo.Database, cx context.Context, incTraceCode string, mapIncoming map[string]interface{}) (string, string, []map[string]interface{}) {
	var mapReturns []map[string]interface{}
	responseStatus := "000"
	responseDesc := "Success"
	//isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
	collectionName := incClientID + "_" + incFormulaID

	collection := dbMongo.Collection(collectionName)
	filter := bson.D{{Key: "formula_id", Value: incFormulaID}}

	cursor, errC := collection.Find(cx, filter)
	if errC != nil {
		responseStatus = "400"
		responseDesc = "Data not found - filter problem"
		panic(errC)
	}
	//var results []map[string]interface{}
	errC = cursor.All(cx, &mapReturns)
	if errC != nil {
		responseStatus = "400"
		responseDesc = "Data not found - data empty"
		panic(errC)
	}
	return responseStatus, responseDesc, mapReturns
}

func getOneFormulaDataFromPostgres(dbPostgres *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (string, map[string]interface{}) {
	mapReturn := make(map[string]interface{})
	responseStatus := "400"

	//isSuccess := false

	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
	incDataID := modules.GetStringFromMapInterface(mapIncoming, "dataid")

	query := `SELECT data_id, yt2.client_id, yt2.formula_id, f.formula_name, process_id, results, data_receive_datetime, data_process_datetime, 
        data_receive_code, is_process FROM ytransaction_v2 yt2 LEFT JOIN yformula_v3 f on yt2.formula_id = f.formula_id WHERE yt2.formula_id = $1 AND data_id = $2 LIMIT 1`

	rows, err := dbPostgres.Query(query, incFormulaID, incDataID)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to insert tables. Error occur.", true, err)
	} else {

		defer rows.Close()

		for rows.Next() {

			var formulaId sql.NullString
			var formulaName sql.NullString
			var clientId sql.NullString
			var dataId sql.NullString
			var processId sql.NullString
			var results sql.NullString
			var dataReceiveDatetime sql.NullTime
			var dataProcessDatetime sql.NullTime
			var dataReceiveCode sql.NullString
			var isProcess sql.NullBool

			errS := rows.Scan(&dataId, &clientId, &formulaId, &formulaName, &processId, &results,
				&dataReceiveDatetime, &dataProcessDatetime, &dataReceiveCode, &isProcess)

			if errS != nil {
				modules.DoLog("INFO", "", "LandingGRPC", "Package",
					"Failed to read database. Error occur.", true, errS)
				responseStatus = "901"
			} else {
				responseStatus = "000"
				strDataId := modules.ConvertSQLNullStringToString(dataId)
				strClientId := modules.ConvertSQLNullStringToString(clientId)
				strFormulaId := modules.ConvertSQLNullStringToString(formulaId)
				strFormulaName := modules.ConvertSQLNullStringToString(formulaName)
				strProcessId := modules.ConvertSQLNullStringToString(processId)
				strResults := modules.ConvertSQLNullStringToString(results)
				timeFormulaCreateDatetime := modules.ConvertSQLNullTimeToTime(dataReceiveDatetime)
				timeFormulaUpdateDatetime := modules.ConvertSQLNullTimeToTime(dataProcessDatetime)
				strDataReceiveCode := modules.ConvertSQLNullStringToString(dataReceiveCode)
				boolIsProcess := modules.ConvertSQLNullBoolToBool(isProcess)

				mapReturn["data_id"] = strDataId
				mapReturn["client_id"] = strClientId
				mapReturn["formula_id"] = strFormulaId
				mapReturn["formula_name"] = strFormulaName
				mapReturn["process_id"] = strProcessId
				mapReturn["results"] = strResults
				mapReturn["data_receive_datetime"] = timeFormulaCreateDatetime
				mapReturn["data_process_datetime"] = timeFormulaUpdateDatetime
				mapReturn["data_receive_code"] = strDataReceiveCode
				mapReturn["is_process"] = boolIsProcess
			}
		}
	}
	return responseStatus, mapReturn
}

func ProcessGetAll(dbPostgres *sql.DB, dbMongo *mongo.Database, redisClient *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapDataResults := make(map[string]interface{})
	mapResponse := make(map[string]interface{})
	postgresResults := make(map[string]interface{})
	var finalResults []map[string]interface{}
	var mongoResults []map[string]interface{}

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
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

		if len(incUsername) > 0 && len(incClientID) > 0 && len(incFormulaID) > 0 && len(incAccessToken) > 0 {

			isCredentialValid := modules.DoCheckRedisCredential(redisClient, cx, incClientID, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				mapDataResults["client_id"] = incClientID
				mapDataResults["formula_id"] = incFormulaID
				respStatus, statusDesc, mongoResults = getAllFormulaDataFromMongo(dbMongo, cx, incTraceCode, mapIncoming)
				_, mapFormula := APIGetFormula.GetOneFormulaFromPostgres(dbPostgres, incTraceCode, mapIncoming)
				if len(mapFormula) > 0 {
					mapDataResults["fields"] = mapFormula["fields"]
					mapDataResults["formula_name"] = mapFormula["formula_name"]
				} else {
					mapDataResults["fields"] = make([]map[string]interface{}, 0, 0)
					mapDataResults["formula_name"] = ""
				}
				if len(mongoResults) > 0 {
					for _, mongoResult := range mongoResults {
						delete(mongoResult, "_id") // Remove unused data json
						finalResult := make(map[string]interface{})

						mapIncoming["dataid"] = mongoResult["data_id"]
						//finalResult["data_id"] = mongoResult["data_id"]
						//finalResult["client_id"] = mongoResult["client_id"]
						//finalResult["formula_id"] = mongoResult["formula_id"]
						//finalResult["data_receive_datetime"] = mongoResult["data_receive_datetime"]
						//finalResult["data_details"] = mongoResult
						for key, value := range mongoResult {
							finalResult[key] = value
						}

						respStatus, postgresResults = getOneFormulaDataFromPostgres(dbPostgres, incTraceCode, mapIncoming)

						if len(postgresResults) > 0 {
							finalResult["formula_name"] = postgresResults["formula_name"]
							finalResult["process_id"] = postgresResults["process_id"]
							//finalResult["results"] = modules.ConvertJSONStringToMap(finalResult["data_id"].(string), postgresResults["results"].(string))
							mapResultPostgres := modules.ConvertJSONStringToMap(finalResult["data_id"].(string), postgresResults["results"].(string))
							for key, value := range mapResultPostgres {
								finalResult[key] = value
							}
							finalResult["data_process_datetime"] = postgresResults["data_process_datetime"]
							finalResult["data_receive_code"] = postgresResults["data_receive_code"]
							finalResult["is_process"] = postgresResults["is_process"]
						} else {
							finalResult["formula_name"] = ""
							finalResult["process_id"] = ""
							//finalResult["results"] = make(map[string]interface{})
							finalResult["data_process_datetime"] = ""
							finalResult["data_receive_code"] = ""
							finalResult["is_process"] = false
						}
						delete(mongoResult, "_id")                   // Remove unused data json
						delete(mongoResult, "client_id")             // Remove unused data json
						delete(mongoResult, "formula_id")            // Remove unused data json
						delete(mongoResult, "process_id")            // Remove unused data json
						delete(mongoResult, "data_receive_datetime") // Remove unused data json
						delete(mongoResult, "data_receive_code")     // Remove unused data json
						delete(mongoResult, "is_process")            // Remove unused data json
						finalResults = append(finalResults, finalResult)
					}
					mapDataResults["results"] = finalResults
				} else {
					statusDesc = "Data empty"
				}
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

	mapResponse["data"] = mapDataResults
	mapResponse["description"] = statusDesc
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}

func getOneFormulaDataFromMongo(dbMongo *mongo.Database, cx context.Context, incTraceCode string, mapIncoming map[string]interface{}) (string, string, []map[string]interface{}) {
	var mapReturns []map[string]interface{}
	responseStatus := "000"
	responseDesc := "Success"
	modules.DoLog("INFO", incTraceCode, "APIGetData", "getOneFormulaDataFromMongo",
		fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
	incDataID := modules.GetStringFromMapInterface(mapIncoming, "dataid")
	modules.DoLog("INFO", incTraceCode, "APIGetData", "getOneFormulaDataFromMongo",
		fmt.Sprintf("incClientID: %+v", incClientID)+", "+fmt.Sprintf("incFormulaID: %+v", incFormulaID)+
			", "+fmt.Sprintf("incDataID: %+v", incDataID), false, nil)
	collectionName := incClientID + "_" + incFormulaID

	collection := dbMongo.Collection(collectionName)
	filter := bson.D{{Key: "formula_id", Value: incFormulaID}, {Key: "data_id", Value: incDataID}}
	modules.DoLog("INFO", incTraceCode, "APIGetData", "getOneFormulaDataFromMongo",
		fmt.Sprintf("filter: %+v", filter), false, nil)

	cursor, errC := collection.Find(cx, filter)
	if errC != nil {
		responseStatus = "400"
		responseDesc = "Data not found - filter problem"
		panic(errC)
	}
	errC = cursor.All(cx, &mapReturns)
	if errC != nil {
		responseStatus = "400"
		responseDesc = "Data not found - data empty"
		panic(errC)
	}
	return responseStatus, responseDesc, mapReturns
}

func ProcessGetById(dbPostgres *sql.DB, dbMongo *mongo.Database, redisClient *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapDataResults := make(map[string]interface{})
	mapResponse := make(map[string]interface{})
	postgresResults := make(map[string]interface{})
	var finalResults []map[string]interface{}
	var mongoResults []map[string]interface{}

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
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")
		incDataID := modules.GetStringFromMapInterface(mapIncoming, "dataid")

		if len(incUsername) > 0 && len(incFormulaID) > 0 && len(incDataID) > 0 && len(incClientID) > 0 && len(incAccessToken) > 0 {

			isCredentialValid := modules.DoCheckRedisCredential(redisClient, cx, incClientID, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {
				mapDataResults["client_id"] = incClientID
				mapDataResults["formula_id"] = incFormulaID
				respStatus, statusDesc, mongoResults = getOneFormulaDataFromMongo(dbMongo, cx, incTraceCode, mapIncoming)
				_, mapFormula := APIGetFormula.GetOneFormulaFromPostgres(dbPostgres, incTraceCode, mapIncoming)
				if len(mapFormula) > 0 {
					mapDataResults["fields"] = mapFormula["fields"]
					mapDataResults["formula_name"] = mapFormula["formula_name"]
				} else {
					mapDataResults["fields"] = make([]map[string]interface{}, 0, 0)
					mapDataResults["formula_name"] = ""
				}
				modules.DoLog("INFO", incTraceCode, "APIGetData", "ProcessGetById",
					fmt.Sprintf("mongoResults: %+v", mongoResults), false, nil)
				if len(mongoResults) > 0 {
					for _, mongoResult := range mongoResults {
						finalResult := make(map[string]interface{})

						mapIncoming["dataid"] = mongoResult["data_id"]
						//finalResult["data_id"] = mongoResult["data_id"]
						//finalResult["client_id"] = mongoResult["client_id"]
						//finalResult["formula_id"] = mongoResult["formula_id"]
						//finalResult["data_receive_datetime"] = mongoResult["data_receive_datetime"]
						//finalResult["fields"] = mongoResult
						for key, value := range mongoResult {
							finalResult[key] = value
						}

						respStatus, postgresResults = getOneFormulaDataFromPostgres(dbPostgres, incTraceCode, mapIncoming)

						if len(postgresResults) > 0 {
							finalResult["formula_name"] = postgresResults["formula_name"]
							finalResult["process_id"] = postgresResults["process_id"]
							//finalResult["results"] = modules.ConvertJSONStringToMap(finalResult["data_id"].(string), postgresResults["results"].(string))
							mapResultPostgres := modules.ConvertJSONStringToMap(finalResult["data_id"].(string), postgresResults["results"].(string))
							for key, value := range mapResultPostgres {
								finalResult[key] = value
							}
							finalResult["data_process_datetime"] = postgresResults["data_process_datetime"]
							finalResult["data_receive_code"] = postgresResults["data_receive_code"]
							finalResult["is_process"] = postgresResults["is_process"]
						} else {
							finalResult["formula_name"] = ""
							finalResult["process_id"] = ""
							//finalResult["results"] = make(map[string]interface{})
							finalResult["data_process_datetime"] = ""
							finalResult["data_receive_code"] = ""
							finalResult["is_process"] = false
						}
						delete(mongoResult, "_id")                   // Remove unused data json
						delete(mongoResult, "client_id")             // Remove unused data json
						delete(mongoResult, "formula_id")            // Remove unused data json
						delete(mongoResult, "process_id")            // Remove unused data json
						delete(mongoResult, "data_receive_datetime") // Remove unused data json
						delete(mongoResult, "data_receive_code")     // Remove unused data json
						delete(mongoResult, "is_process")            // Remove unused data json
						finalResults = append(finalResults, finalResult)
					}
					mapDataResults["results"] = finalResults
				} else {
					statusDesc = "Data empty"
				}
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

	mapResponse["data"] = mapDataResults
	mapResponse["description"] = statusDesc
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
