package CronProcessed

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/go-redis/redis/v8"
	"strconv"
	"strings"
	"time"
)

func updateDatabase(db *sql.DB, incTraceCode string, incField string, incResult interface{}, incDataID string, incProcessID string, incClientID string) {

	//incDateTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := "UPDATE ydata " +
		"SET process_id = $1, " + incField + " = $2 WHERE data_id = $3 AND client_id = $4"

	result, err := db.Exec(query, incProcessID, incResult, incDataID, incClientID)

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
			fmt.Println("RESULT "+incTraceCode+" ===> ", incResult)
			fmt.Println("")
		} else {
			isSuccess = false
		}
	} else {
		isSuccess = false
	}

	return isSuccess, incDataID
}

func GetData(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incClientID string, incFormulaID string, redisKey string) {

	var mapData = make(map[string]interface{})
	incDataID := ""

	queryX := "SELECT client_id, formula_id FROM ydata " +
		"WHERE client_id LIKE $1 AND formula_id LIKE $2"

	rowsX, errX := db.Query(queryX, "%"+incClientID+"%", "%"+incFormulaID+"%")

	if errX != nil {
		fmt.Println("FAILED : ", errX)
	} else {
		for rowsX.Next() {
			var rawClientID sql.NullString
			var rawFormulaID sql.NullString

			errPX := rowsX.Scan(&rawClientID, &rawFormulaID)

			if errPX != nil {
				fmt.Println("FAILED : ", errPX)
			} else {
				strClientID := modules.ConvertSQLNullStringToString(rawClientID)
				strFormulaID := modules.ConvertSQLNullStringToString(rawFormulaID)

				//redisKey := "test_" + strFormulaID
				redisVal, _ := modules.RedisGet(rc, cx, redisKey)
				mapRedis := modules.ConvertJSONStringToMap("", redisVal)

				//strClient := modules.GetStringFromMapInterface(mapRedis, "client")
				arrFormula := mapRedis["formula"].([]interface{})
				arrResult := mapRedis["result"].([]interface{})

				//strType := modules.GetStringFromMapInterface(mapRedis, "type")
				//strTime := modules.GetStringFromMapInterface(mapRedis, "time")

				//var strResult string

				incProcessID := modules.GenerateUUID()

				for x := 0; x < len(arrFormula); x++ {

					theFormula := fmt.Sprintf("%v", arrFormula[x])
					theResult := fmt.Sprintf("%v", arrResult[x])

					query := "SELECT f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, " +
						"f11, f12, f13, f14, f15, f16, f17, f18, f19, f20," +
						"f21, f22, f23, f24, f25, f26, f27, f28, f29, f30," +
						"f31, f32, f33, f34, f35, f36, f37, f38, f39, f40," +
						"f41, f42, f43, f44, f45, f46, f47, f48, f49, f50," +
						"data_id " +
						"FROM ydata WHERE is_process = false AND client_id = $1 AND formula_id = $2"

					rows, err := db.Query(query, strClientID, strFormulaID)

					if err != nil {
						fmt.Println("FAILED : ", err)
					} else {
						for rows.Next() {
							var rawF [50]sql.NullString
							var rawDataID sql.NullString

							errP := rows.Scan(&rawF[0], &rawF[1], &rawF[2], &rawF[3], &rawF[4], &rawF[5], &rawF[6], &rawF[7], &rawF[8], &rawF[9], &rawF[10],
								&rawF[11], &rawF[12], &rawF[13], &rawF[14], &rawF[15], &rawF[16], &rawF[17], &rawF[18], &rawF[19], &rawF[20],
								&rawF[21], &rawF[22], &rawF[23], &rawF[24], &rawF[25], &rawF[26], &rawF[27], &rawF[28], &rawF[29], &rawF[30],
								&rawF[31], &rawF[32], &rawF[33], &rawF[34], &rawF[35], &rawF[36], &rawF[37], &rawF[38], &rawF[39], &rawF[40],
								&rawF[41], &rawF[42], &rawF[43], &rawF[44], &rawF[45], &rawF[46], &rawF[47], &rawF[48], &rawF[49],
								&rawDataID)

							if errP != nil {
								fmt.Println("FAILED : ", errP)
							} else {

								for x := 0; x < 50; x++ {
									strF := modules.ConvertSQLNullStringToString(rawF[x])
									fltF, errParse := strconv.ParseFloat(strF, 64)
									if errParse == nil {
										mapData["f"+fmt.Sprintf("%v", x+1)] = fltF
									} else {
										mapData["f"+fmt.Sprintf("%v", x+1)] = strF
									}
								}
								incDataID = modules.ConvertSQLNullStringToString(rawDataID)
								mapData["dataid"] = incDataID
								mapData["processid"] = incProcessID

								_, _ = processForNonString(db, rc, cx, incTraceCode, strClientID, strFormulaID, theFormula, theResult, mapData)
							}
						}
					}
				}
				// Bila akan di kumpulkan datanya di table lain
				_, _ = processSavetoAnotherDB(db, rc, cx, incTraceCode, incDataID)

			}
		}
	}

}

func processSavetoAnotherDB(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incDataID string) (bool, string) {

	query := `INSERT INTO ytransaction 
		SELECT data_id, client_id, formula_id, process_id, 
		f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, 
		f11, f12, f13, f14, f15, f16, f17, f18, f19, f20,
		f21, f22, f23, f24, f25, f26, f27, f28, f29, f30,
		f31, f32, f33, f34, f35, f36, f37, f38, f39, f40,
		f41, f42, f43, f44, f45, f46, f47, f48, f49, f50 
		FROM ydata WHERE data_id = $1`

	result, err := db.Exec(query, incDataID)

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

	return true, ""
}

func getSUM(db *sql.DB, incFormula string, incProcessID string) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "SUM", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	query := "SELECT SUM(" + rawFormula + "::NUMERIC) FROM ydata " +
		"WHERE process_id = $1"

	rows, err := db.Query(query, incProcessID)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawTotal sql.NullFloat64

			errP := rows.Scan(&rawTotal)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				fltResult = modules.ConvertSQLNullFloat64ToFloat64(rawTotal)
			}
		}
	}
	return fltResult
}

func getAVG(db *sql.DB, incFormula string, incProcessID string) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "AVG", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	query := "SELECT AVG(" + rawFormula + "::NUMERIC) FROM ydata " +
		"WHERE process_id = $1"

	rows, err := db.Query(query, incProcessID)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawTotal sql.NullFloat64

			errP := rows.Scan(&rawTotal)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				fltResult = modules.ConvertSQLNullFloat64ToFloat64(rawTotal)
			}
		}
	}
	return fltResult
}

func getCOUNT(db *sql.DB, incFormula string, incProcessID string) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "COUNT", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	query := "SELECT COUNT(" + rawFormula + "::NUMERIC) FROM ydata " +
		"WHERE process_id = $1"

	rows, err := db.Query(query, incProcessID)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawTotal sql.NullFloat64

			errP := rows.Scan(&rawTotal)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				fltResult = modules.ConvertSQLNullFloat64ToFloat64(rawTotal)
			}
		}
	}
	return fltResult
}

func getMAX(db *sql.DB, incFormula string, incProcessID string) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "MAX", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	query := "SELECT MAX(" + rawFormula + "::NUMERIC) FROM ydata " +
		"WHERE process_id = $1"

	rows, err := db.Query(query, incProcessID)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawTotal sql.NullFloat64

			errP := rows.Scan(&rawTotal)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				fltResult = modules.ConvertSQLNullFloat64ToFloat64(rawTotal)
			}
		}
	}
	return fltResult
}

func getMIN(db *sql.DB, incFormula string, incProcessID string) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "MIN", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	query := "SELECT MIN(" + rawFormula + "::NUMERIC) FROM ydata " +
		"WHERE process_id = $1"

	rows, err := db.Query(query, incProcessID)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawTotal sql.NullFloat64

			errP := rows.Scan(&rawTotal)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				fltResult = modules.ConvertSQLNullFloat64ToFloat64(rawTotal)
			}
		}
	}
	return fltResult
}

func checkFunction(db *sql.DB, incFormula string, incProcessID string) (bool, interface{}) {
	isValid := true
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	if strings.Contains(rawFormula, "SUM") {
		fltResult = getSUM(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "AVG") {
		fltResult = getAVG(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "COUNT") {
		fltResult = getCOUNT(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "MAX") {
		fltResult = getMAX(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "MIN") {
		fltResult = getMIN(db, incFormula, incProcessID)
	} else {
		isValid = false
	}
	return isValid, fltResult
}

func processForNonString(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incClientID string, incFormulaID string, incFormula string, incField string, mapData map[string]interface{}) (bool, string) {

	isSuccess := false
	incDataID := modules.GetStringFromMapInterface(mapData, "dataid")
	incProcessID := modules.GetStringFromMapInterface(mapData, "processid")

	isFunction, incResult := checkFunction(db, incFormula, incProcessID)
	if !isFunction {
		expression, _ := govaluate.NewEvaluableExpression(incFormula)
		fmt.Println(expression)
		rawResult, err := expression.Evaluate(mapData)
		incResult = fmt.Sprintf("%v", rawResult)

		if err == nil {
			updateDatabase(db, "incTraceCode", incField, incResult, incDataID, incProcessID, incClientID)
			//updateDatabase(db, "", incField, incResult, incDataID, incClientID, incProcessID)
			//if isSaveSuccess {
			//	isSuccess = true
			//	fmt.Println("RESULT " + incTraceCode + " ===> ", incResult)
			//	fmt.Println("")
			//} else {
			//	isSuccess = false
			//}
		} else {
			isSuccess = false
		}
	} else {
		updateDatabase(db, "incTraceCode", incField, incResult, incDataID, incProcessID, incClientID)

	}
	fmt.Println("Result : ", incResult)

	return isSuccess, incDataID
}
