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

	incField21 := modules.GetStringFromMapInterface(mapIncoming, "par21")
	incField22 := modules.GetStringFromMapInterface(mapIncoming, "par22")
	incField23 := modules.GetStringFromMapInterface(mapIncoming, "par23")
	incField24 := modules.GetStringFromMapInterface(mapIncoming, "par24")
	incField25 := modules.GetStringFromMapInterface(mapIncoming, "par25")
	incField26 := modules.GetStringFromMapInterface(mapIncoming, "par26")
	incField27 := modules.GetStringFromMapInterface(mapIncoming, "par27")
	incField28 := modules.GetStringFromMapInterface(mapIncoming, "par28")
	incField29 := modules.GetStringFromMapInterface(mapIncoming, "par29")
	incField30 := modules.GetStringFromMapInterface(mapIncoming, "par30")

	incField31 := modules.GetStringFromMapInterface(mapIncoming, "par31")
	incField32 := modules.GetStringFromMapInterface(mapIncoming, "par32")
	incField33 := modules.GetStringFromMapInterface(mapIncoming, "par33")
	incField34 := modules.GetStringFromMapInterface(mapIncoming, "par34")
	incField35 := modules.GetStringFromMapInterface(mapIncoming, "par35")
	incField36 := modules.GetStringFromMapInterface(mapIncoming, "par36")
	incField37 := modules.GetStringFromMapInterface(mapIncoming, "par37")
	incField38 := modules.GetStringFromMapInterface(mapIncoming, "par38")
	incField39 := modules.GetStringFromMapInterface(mapIncoming, "par39")
	incField40 := modules.GetStringFromMapInterface(mapIncoming, "par40")

	incField41 := modules.GetStringFromMapInterface(mapIncoming, "par41")
	incField42 := modules.GetStringFromMapInterface(mapIncoming, "par42")
	incField43 := modules.GetStringFromMapInterface(mapIncoming, "par43")
	incField44 := modules.GetStringFromMapInterface(mapIncoming, "par44")
	incField45 := modules.GetStringFromMapInterface(mapIncoming, "par45")
	incField46 := modules.GetStringFromMapInterface(mapIncoming, "par46")
	incField47 := modules.GetStringFromMapInterface(mapIncoming, "par47")
	incField48 := modules.GetStringFromMapInterface(mapIncoming, "par48")
	incField49 := modules.GetStringFromMapInterface(mapIncoming, "par49")
	incField50 := modules.GetStringFromMapInterface(mapIncoming, "par50")

	incFormula := modules.GetStringFromMapInterface(mapIncoming, "formula")
	incFormulaName := modules.GetStringFromMapInterface(mapIncoming, "name")
	incType := modules.GetStringFromMapInterface(mapIncoming, "type")
	incTime := modules.GetStringFromMapInterface(mapIncoming, "time")

	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := `UPDATE yformula 
		SET formula_name = $3,  
		f1 = $4, f2 = $5, f3 = $6, f4 = $7, f5 = $8, f6 = $9, f7 = $10, f8 = $11, f9 = $12, f10 = $13, 
		f11 = $14, f12 = $15, f13 = $16, f14 = $17, f15 = $18, f16 = $19, f17 = $20, f18 = $21, f19 = $22, f20 = $23, 
		f21 = $24, f22 = $25, f23 = $26, f24 = $27, f25 = $28, f26 = $29, f27 = $30, f28 = $31, f29 = $32, f30 = $33, 
		f31 = $34, f32 = $35, f33 = $36, f34 = $37, f35 = $38, f36 = $39, f37 = $40, f38 = $41, f39 = $42, f40 = $43, 
		f41 = $44, f42 = $45, f43 = $46, f44 = $47, f45 = $48, f46 = $49, f47 = $50, f48 = $51, f49 = $52, f50 = $53, 
		formula = $54, formula_type = $55, formula_time = $56, formula_update_datetime = $57, is_active = $58  
		WHERE client_id = $1 AND formula_id = $2`

	result, err := db.Exec(query, incClientID, incFormulaID, incFormulaName,
		incField1, incField2, incField3, incField4, incField5, incField6, incField7, incField8, incField9, incField10,
		incField11, incField12, incField13, incField14, incField15, incField16, incField17, incField18, incField19, incField20,
		incField21, incField22, incField23, incField24, incField25, incField26, incField27, incField28, incField29, incField30,
		incField31, incField32, incField33, incField34, incField35, incField36, incField37, incField38, incField39, incField40,
		incField41, incField42, incField43, incField44, incField45, incField46, incField47, incField48, incField49, incField50,
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

	if len(mapIncoming) > 0 {

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
