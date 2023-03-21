package CronProcessed

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/go-redis/redis/v8"
	"time"
)

func updateDatabase(db *sql.DB, incTraceCode string, incDataID string) {

	incDateTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "UPDATE client_data " +
		"SET data_process_datetime = $2, is_process = $3 WHERE data_id = $1"

	result, err := db.Exec(query, incDataID, incDateTimeNow, true)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "Cron", "Processed",
			"Failed to update tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			modules.DoLog("INFO", incTraceCode, "Cron", "Processed",
				"Success to insert tables.", true, nil)
		} else {
			modules.DoLog("ERROR", incTraceCode, "Cron", "Processed",
				"Failed to insert tables. Error occur.", true, err)
		}
	}
}

func saveToDatabase(db *sql.DB, incTraceCode string, incDataID string, incTransactionID string, incClientID string, incFormulaID string, incResult interface{}) bool {

	isSuccess := false
	incDateTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "INSERT INTO formula_result (data_id, transaction_id, client_id, formula_id, result, result_process_datetime) " +
		"VALUES ($1,$2,$3,$4,$5,$6)"

	result, err := db.Exec(query, incDataID, incTransactionID, incClientID, incFormulaID, incResult, incDateTimeNow)

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


func getResult(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incTransactionID string, incClientID string, incFormulaID string, incFormula string, mapData map[string]interface{}) (bool, string) {

	isSuccess := false
	incDataID := modules.GetStringFromMapInterface(mapData, "dataid")
	expression, _ := govaluate.NewEvaluableExpression(incFormula)
	fmt.Println("Conversion : ", expression)
	incResult, err := expression.Evaluate(mapData)

	if err == nil {
		isSaveSuccess := saveToDatabase(db, incTraceCode, incDataID, incTransactionID, incClientID, incFormulaID, incResult)
		if isSaveSuccess {
			isSuccess = true
			fmt.Println("RESULT " + incTraceCode + " ===> ", incResult)
			fmt.Println("")
		} else {
			isSuccess = false
		}
	} else {
		isSuccess = false
	}

	return isSuccess, incDataID
}

func GetData(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incTransactionID string, incCLientID string, incFormulaID string, incFormula string) {

	var mapData = make(map[string]interface{})

	query := "SELECT field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, " +
		"field11, field12, field13, field14, field15, field16, field17, field18, field19, field20," +
		"data_id " +
		"FROM client_data WHERE is_process = false AND formula_id = $1"

	rows, err := db.Query(query, incFormulaID)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawField [20]sql.NullFloat64
			var rawDataID sql.NullString

			errP := rows.Scan(&rawField[0], &rawField[1], &rawField[2], &rawField[3], &rawField[4], &rawField[5],
				&rawField[6], &rawField[7], &rawField[8], &rawField[9], &rawField[10], &rawField[11], &rawField[12],
				&rawField[13], &rawField[14], &rawField[15], &rawField[16], &rawField[17], &rawField[18], &rawField[19],
				&rawDataID)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {

				for x := 0; x < 20; x++ {
					mapData["field" + fmt.Sprintf("%v", x+1)] = modules.ConvertSQLNullFloat64ToFloat64(rawField[x])
				}

				mapData["dataid"] = modules.ConvertSQLNullStringToString(rawDataID)

				isSuccess, incDataID := getResult(db, rc, cx, incTraceCode, incTransactionID, incCLientID, incFormulaID, incFormula, mapData)
				if isSuccess {
					updateDatabase(db, incTraceCode, incDataID)
				}

			}
		}
	}
}

