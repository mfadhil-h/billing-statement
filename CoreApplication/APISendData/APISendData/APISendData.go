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

	incDataID := modules.GetStringFromMapInterface(mapIncoming, "dataid")
	incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
	incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

	incDatas := mapIncoming["datas"].(map[string]interface{})
	jsonDatas := modules.ConvertMapInterfaceToJSON(incDatas)

	//incDatas := mapIncoming["datas"].(interface{})

	//incField1 := modules.GetStringFromMapInterface(mapIncoming, "data1")
	//incField2 := modules.GetStringFromMapInterface(mapIncoming, "data2")
	//incField3 := modules.GetStringFromMapInterface(mapIncoming, "data3")
	//incField4 := modules.GetStringFromMapInterface(mapIncoming, "data4")
	//incField5 := modules.GetStringFromMapInterface(mapIncoming, "data5")
	//incField6 := modules.GetStringFromMapInterface(mapIncoming, "data6")
	//incField7 := modules.GetStringFromMapInterface(mapIncoming, "data7")
	//incField8 := modules.GetStringFromMapInterface(mapIncoming, "data8")
	//incField9 := modules.GetStringFromMapInterface(mapIncoming, "data9")
	//incField10 := modules.GetStringFromMapInterface(mapIncoming, "data10")
	//
	//incField11 := modules.GetStringFromMapInterface(mapIncoming, "data11")
	//incField12 := modules.GetStringFromMapInterface(mapIncoming, "data12")
	//incField13 := modules.GetStringFromMapInterface(mapIncoming, "data13")
	//incField14 := modules.GetStringFromMapInterface(mapIncoming, "data14")
	//incField15 := modules.GetStringFromMapInterface(mapIncoming, "data15")
	//incField16 := modules.GetStringFromMapInterface(mapIncoming, "data16")
	//incField17 := modules.GetStringFromMapInterface(mapIncoming, "data17")
	//incField18 := modules.GetStringFromMapInterface(mapIncoming, "data18")
	//incField19 := modules.GetStringFromMapInterface(mapIncoming, "data19")
	//incField20 := modules.GetStringFromMapInterface(mapIncoming, "data20")
	//
	//incField21 := modules.GetStringFromMapInterface(mapIncoming, "data21")
	//incField22 := modules.GetStringFromMapInterface(mapIncoming, "data22")
	//incField23 := modules.GetStringFromMapInterface(mapIncoming, "data23")
	//incField24 := modules.GetStringFromMapInterface(mapIncoming, "data24")
	//incField25 := modules.GetStringFromMapInterface(mapIncoming, "data25")
	//incField26 := modules.GetStringFromMapInterface(mapIncoming, "data26")
	//incField27 := modules.GetStringFromMapInterface(mapIncoming, "data27")
	//incField28 := modules.GetStringFromMapInterface(mapIncoming, "data28")
	//incField29 := modules.GetStringFromMapInterface(mapIncoming, "data29")
	//incField30 := modules.GetStringFromMapInterface(mapIncoming, "data30")
	//
	//incField31 := modules.GetStringFromMapInterface(mapIncoming, "data31")
	//incField32 := modules.GetStringFromMapInterface(mapIncoming, "data32")
	//incField33 := modules.GetStringFromMapInterface(mapIncoming, "data33")
	//incField34 := modules.GetStringFromMapInterface(mapIncoming, "data34")
	//incField35 := modules.GetStringFromMapInterface(mapIncoming, "data35")
	//incField36 := modules.GetStringFromMapInterface(mapIncoming, "data36")
	//incField37 := modules.GetStringFromMapInterface(mapIncoming, "data37")
	//incField38 := modules.GetStringFromMapInterface(mapIncoming, "data38")
	//incField39 := modules.GetStringFromMapInterface(mapIncoming, "data39")
	//incField40 := modules.GetStringFromMapInterface(mapIncoming, "data40")
	//
	//incField41 := modules.GetStringFromMapInterface(mapIncoming, "data41")
	//incField42 := modules.GetStringFromMapInterface(mapIncoming, "data42")
	//incField43 := modules.GetStringFromMapInterface(mapIncoming, "data43")
	//incField44 := modules.GetStringFromMapInterface(mapIncoming, "data44")
	//incField45 := modules.GetStringFromMapInterface(mapIncoming, "data45")
	//incField46 := modules.GetStringFromMapInterface(mapIncoming, "data46")
	//incField47 := modules.GetStringFromMapInterface(mapIncoming, "data47")
	//incField48 := modules.GetStringFromMapInterface(mapIncoming, "data48")
	//incField49 := modules.GetStringFromMapInterface(mapIncoming, "data49")
	//incField50 := modules.GetStringFromMapInterface(mapIncoming, "data50")

	incProcessID := modules.GenerateUUID()
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	//query := `INSERT INTO ydata_v2 (data_id, client_id, formula_id, process_id,
	//	f1, f2, f3, f4, f5, f6, f7, f8, f9, f10,
	//	f11, f12, f13, f14, f15, f16, f17, f18, f19, f20,
	//    f21, f22, f23, f24, f25, f26, f27, f28, f29, f30,
	//    f31, f32, f33, f34, f35, f36, f37, f38, f39, f40,
	//    f41, f42, f43, f44, f45, f46, f47, f48, f49, f50,
	//	data_receive_datetime, data_receive_code, is_process)
	//	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,
	//	$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,
	//	$55,$56,$57)`
	//
	//result, err := db.Exec(query, incDataID, incClientID, incFormulaID, incProcessID,
	//	incField1, incField2, incField3, incField4, incField5, incField6, incField7, incField8, incField9, incField10,
	//	incField11, incField12, incField13, incField14, incField15, incField16, incField17, incField18, incField19, incField20,
	//	incField21, incField22, incField23, incField24, incField25, incField26, incField27, incField28, incField29, incField30,
	//	incField31, incField32, incField33, incField34, incField35, incField36, incField37, incField38, incField39, incField40,
	//	incField41, incField42, incField43, incField44, incField45, incField46, incField47, incField48, incField49, incField50,
	//	incTimeNow, incTraceCode, false)

	query := `INSERT INTO ydata_v2 (data_id, client_id, formula_id, process_id, datas, data_receive_datetime, 
        data_receive_code, is_process) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`

	result, err := db.Exec(query, incDataID, incClientID, incFormulaID, incProcessID, jsonDatas, incTimeNow,
		incTraceCode, false)

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

	if len(mapIncoming) > 0 {

		modules.DoLog("INFO", incTraceCode, "API", "Auth",
			fmt.Sprintf("mapIncoming: %+v", mapIncoming), false, nil)
		/* TEMP REMOVE AUTH FOR TEST */
		incClientID := modules.GetStringFromMapInterface(mapIncoming, "clientid")
		//incUsername := modules.GetStringFromMapInterface(mapIncoming, "username")
		//incPassword := modules.GetStringFromMapInterface(mapIncoming, "password")
		//incKey := modules.GetStringFromMapInterface(mapIncoming, "key")
		incFormulaID := modules.GetStringFromMapInterface(mapIncoming, "formulaid")

		//isCredentialValid := modules.DoCheckRedisClientHit(rc, cx, incClientID, incUsername, incPassword, incKey, incRemoteIPAddress)
		isValid := modules.DoCheckFormulaID(rc, cx, incFormulaID)

		//if len(incUsername) > 0 && len(incPassword) > 0 && len(incClientID) > 0 && len(incFormulaID) > 0 && isValid && isCredentialValid {
		if len(incClientID) > 0 && len(incFormulaID) > 0 && isValid {

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
