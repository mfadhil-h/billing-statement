package APINewClient

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	moduleName = "APINewClient"
)

func saveToDatabase(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (bool, string) {
	const functionName = "saveToDatabase"

	isSuccess := false

	incGroupID := modules.GetStringFromMapInterface(mapIncoming, "groupid")
	incName := modules.GetStringFromMapInterface(mapIncoming, "name")
	incAddress := modules.GetStringFromMapInterface(mapIncoming, "address")
	incRegion := modules.GetStringFromMapInterface(mapIncoming, "region")
	incCountry := modules.GetStringFromMapInterface(mapIncoming, "country")
	incCurrency := modules.GetStringFromMapInterface(mapIncoming, "currency")
	incEmail := modules.GetStringFromMapInterface(mapIncoming, "email")
	incPhone := modules.GetStringFromMapInterface(mapIncoming, "phone")
	incType := modules.GetStringFromMapInterface(mapIncoming, "type")
	incPicName := modules.GetStringFromMapInterface(mapIncoming, "picname")
	incPicEmail := modules.GetStringFromMapInterface(mapIncoming, "picemail")
	incPicPhone := modules.GetStringFromMapInterface(mapIncoming, "picphone")

	incClientID := modules.GenerateClientID(incName)
	modules.DoLog("INFO", incTraceCode, moduleName, functionName,
		fmt.Sprintf("incClientID: %+v", incClientID), true, nil)
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := `INSERT INTO client (client_id, group_id, client_name, client_address, client_region, client_country, 
                    client_email, client_phone, client_currency, pic_name, pic_email, pic_phone, client_type, 
                    client_create_datetime, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) 
                                                       ON CONFLICT (client_id) DO NOTHING`

	result, err := db.Exec(query, incClientID, incGroupID, incName,
		incAddress, incRegion, incCountry, incEmail, incPhone, incCurrency,
		incPicName, incPicEmail, incPicPhone, incType,
		incTimeNow, true)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
			"Failed to insert tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			isSuccess = true
			modules.DoLog("INFO", incTraceCode, moduleName, functionName,
				"Success to insert tables.", true, nil)
		} else {
			isSuccess = false
			modules.DoLog("ERROR", incTraceCode, moduleName, functionName,
				"Failed to insert tables. Error occur.", true, err)
		}
	}
	return isSuccess, incClientID
}

func NewClientProcess(db *sql.DB, redisClient *redis.Client, contextX context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {
	const functionName = "NewClientProcess"

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})
	result := make(map[string]interface{})

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
		incGroupID := modules.GetStringFromMapInterface(mapIncoming, "groupid")
		incName := modules.GetStringFromMapInterface(mapIncoming, "name")
		incCountry := modules.GetStringFromMapInterface(mapIncoming, "country")
		incCurrency := modules.GetStringFromMapInterface(mapIncoming, "currency")
		incType := modules.GetStringFromMapInterface(mapIncoming, "type")
		incPicName := modules.GetStringFromMapInterface(mapIncoming, "picname")
		incPicEmail := modules.GetStringFromMapInterface(mapIncoming, "picemail")
		incPicPhone := modules.GetStringFromMapInterface(mapIncoming, "picphone")

		if len(incUsername) > 0 && len(incAccessToken) > 0 && len(incGroupID) > 0 && len(incName) > 0 &&
			len(incCountry) > 0 && len(incCurrency) > 0 && len(incType) > 0 && len(incPicName) > 0 &&
			len(incPicEmail) > 0 && len(incPicPhone) > 0 {

			isCredentialValid, _ := modules.DoCheckRedisCredential(redisClient, contextX, incUsername, incAccessToken, incRemoteIPAddress)

			if isCredentialValid {

				isSuccess, strClientID := saveToDatabase(db, incTraceCode, mapIncoming)

				if isSuccess {
					respStatus = "000"
					result["clientid"] = strClientID
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

	mapResponse["description"] = statusDesc
	mapResponse["data"] = result
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime
	mapResponse["tracecode"] = incTraceCode

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
