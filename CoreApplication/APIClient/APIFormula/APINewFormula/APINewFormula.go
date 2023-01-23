package APINewFormula

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
)

func saveToDatabase(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (bool, string) {

	isSuccess := false

	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")

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
	incFormulaID := modules.GenerateFormulaID(incClientID, incFormulaName)
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	if strings.ToUpper(incType) == "REALTIME" {
		incTime = "00:00"
	}

	query := `INSERT INTO yformula (formula_id, client_id, formula_name, 
		f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, 
        f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, 
        f21, f22, f23, f24, f25, f26, f27, f28, f29, f30, 
        f31, f32, f33, f34, f35, f36, f37, f38, f39, f40, 
        f41, f42, f43, f44, f45, f46, f47, f48, f49, f50, 
        formula, formula_type, formula_time, formula_create_datetime, is_active) 
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,
		$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,
		$55,$56,$57,$58)`

	result, err := db.Exec(query, incFormulaID, incClientID, incFormulaName,
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
	return isSuccess, incFormulaID
}

func Process(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string,
	incIncomingHeader map[string]interface{}, mapIncoming map[string]interface{}, incRemoteIPAddress string) (string, map[string]string, string) {

	//incAuthID := modules.GetStringFromMapInterface(incIncomingHeader, "x-data")

	responseHeader := make(map[string]string)
	mapResponse := make(map[string]interface{})

	isTypeValid := false
	respStatus := "900"
	respFormulaID := ""
	responseContent := ""
	respDatetime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now())

	modules.DoLog("INFO", incTraceCode, "API", "Auth",
		"incomingMessage: "+incTraceCode+", remoteIPAddress: "+incRemoteIPAddress, false, nil)

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)
		/* TEMP REMOVE AUTH FOR TEST */

		//incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		//incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		incType := modules.GetStringFromMapInterface(mapIncoming, "type")
		incTime := modules.GetStringFromMapInterface(mapIncoming, "time")

		if strings.ToUpper(incType) != "REALTIME" && len(incTime) == 0 {
			isTypeValid = false
		} else {
			isTypeValid = true
		}

		//if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incType) > 0 && isTypeValid {
		if len(incClientID) > 0 && len(incType) > 0 && isTypeValid {

			isSuccess, strFormulaID := saveToDatabase(db, incTraceCode, mapIncoming)

			if isSuccess {
				modules.ReloadFormulaToRedis(db, rc, cx, strFormulaID)

				respStatus = "000"
				respFormulaID = strFormulaID
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

	mapResponse["formulaid"] = respFormulaID
	mapResponse["status"] = respStatus
	mapResponse["datetime"] = respDatetime

	responseContent = modules.ConvertMapInterfaceToJSON(mapResponse)

	return incTraceCode, responseHeader, responseContent
}
