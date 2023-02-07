package CronProcessed

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"strconv"
	"strings"
	"time"
)

func processSavetoAnotherDB(db *sql.DB, incTraceCode string, incDataID string, incClientID string, incFormulaID string,
	incProcessID string, jsonData string, incTimeReceive string) (bool, string) {
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := `INSERT INTO ytransaction_v2 (data_id, client_id, formula_id, process_id, results, data_receive_datetime, 
        data_process_datetime, data_receive_code, is_process) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`

	result, err := db.Exec(query, incDataID, incClientID, incFormulaID, incProcessID, jsonData, incTimeReceive, incTimeNow,
		incTraceCode, false)

	if err != nil {
		modules.DoLog("ERROR", incTraceCode, "Cron", "Processed",
			"Failed to insert tables. Error occur.", true, err)
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

func checkFunction(incFormula string) (bool, interface{}) {
	isValid := false
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	if strings.Contains(rawFormula, "SUM") {
		//fltResult = getSUM(dbPostgres, incFormula, incProcessID)
		isValid = true
	} else if strings.Contains(rawFormula, "AVG") {
		//fltResult = getAVG(dbPostgres, incFormula, incProcessID)
		isValid = true
	} else if strings.Contains(rawFormula, "COUNT") {
		//fltResult = getCOUNT(dbPostgres, incFormula, incProcessID)
		isValid = true
	} else if strings.Contains(rawFormula, "MAX") {
		//fltResult = getMAX(dbPostgres, incFormula, incProcessID)
		isValid = true
	} else if strings.Contains(rawFormula, "MIN") {
		//fltResult = getMIN(dbPostgres, incFormula, incProcessID)
		isValid = true
	}
	return isValid, fltResult
}

func processForNonString(incFormula string, mapData map[string]interface{}) (bool, bool, string) {

	strResult := ""
	isSuccess := false
	//incDataID := modules.GetStringFromMapInterface(mapData, "dataid")
	//incProcessID := modules.GetStringFromMapInterface(mapData, "processid")

	isFunction, _ := checkFunction(incFormula)
	if !isFunction {
		expression, _ := govaluate.NewEvaluableExpression(incFormula)
		println(fmt.Sprintf("expression: %+v", expression))
		//fmt.Println(expression)
		println(fmt.Sprintf("mapData: %+v", mapData))
		rawResult, err := expression.Evaluate(mapData)
		strResult = fmt.Sprintf("%v", rawResult)
		println(fmt.Sprintf("strResult: %+v", strResult))

		if err == nil {
			//updateDatabase(dbPostgres, "incTraceCode", incField, incResult, incDataID, incProcessID, incClientID)
			//updateDatabase(dbPostgres, "", incField, incResult, incDataID, incClientID, incProcessID)
			//if isSaveSuccess {
			//	isSuccess = true
			//	fmt.Println("RESULT " + incTraceCode + " ===> ", incResult)
			//	fmt.Println("")
			//} else {
			//	isSuccess = false
			//}
		} else {
			fmt.Println("FAILED : ", err)
			isSuccess = false
		}
	} else {
		//updateDatabase(dbPostgres, "incTraceCode", incField, incResult, incDataID, incProcessID, incClientID)

	}
	fmt.Println("Result : ", strResult)

	return isSuccess, isFunction, strResult
}

// GetData /* func in test.go */
func GetData(dbPostgres *sql.DB, dbMongo *mongo.Database, rc *redis.Client, cx context.Context, incTraceCode string,
	incClientID string, incFormulaID string, redisKey string) {

	var mapData []map[string]interface{}
	//incDataID := ""
	isFunctionInFormula := false
	strReceiveDateTime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	//redisKey := "test_" + incFormulaID
	redisVal, _ := modules.RedisGet(rc, cx, redisKey)
	mapRedis := modules.ConvertJSONStringToMap("", redisVal)

	arrFormula := mapRedis["formula"].([]interface{})
	arrResult := mapRedis["result"].([]interface{})
	collectionName := incClientID + "_" + incFormulaID

	incProcessID := modules.GenerateUUID()

	collection := dbMongo.Collection(collectionName)
	filter := bson.D{{Key: "formula_id", Value: incFormulaID}}

	cursor, errC := collection.Find(cx, filter)
	if errC != nil {
		panic(errC)
	}
	var results []map[string]interface{}
	errC = cursor.All(cx, &results)
	if errC != nil {
		panic(errC)
	}
	println(fmt.Sprintf("resulst: %+v", results))
	for _, result := range results {
		checkDataId := modules.GetStringFromMapInterface(result, "data_id")
		checkFormulaId := modules.GetStringFromMapInterface(result, "formula_id")
		checkClientId := modules.GetStringFromMapInterface(result, "client_id")
		existedDataId := modules.GetDataIdByKeys(dbPostgres, checkDataId, checkFormulaId, checkClientId)
		if len(existedDataId) < 1 {
			println("Ready DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapData %+v", result))
			for key, value := range result {
				println(fmt.Sprintf(", \nkey %+v", key))
				if key != "_id" {
					strF := value.(string)
					fltF, errParse := strconv.ParseFloat(strF, 64)
					if errParse == nil {
						result[key] = fltF
					} else {
						result[key] = strF
					}
				}
			}

			for x := 0; x < len(arrFormula); x++ {
				//isSuccess := false
				isFunctionSub := false
				strResult := ""
				theFormula := fmt.Sprintf("%v", arrFormula[x])
				theResult := fmt.Sprintf("%v", arrResult[x])

				//result["dataid"] = incDataID
				result["processid"] = incProcessID

				_, isFunctionSub, strResult = processForNonString(theFormula, result)
				if isFunctionSub {
					result[theResult] = "0"
					isFunctionInFormula = isFunctionSub
				} else {
					fltF, errParse := strconv.ParseFloat(strResult, 64)
					if errParse == nil {
						result[theResult] = fltF
					} else {
						result[theResult] = strResult
					}
				}
			}
			mapData = append(mapData, result)
		}
	}
	if len(mapData) > 0 {
		println("MID DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapData: %+v", mapData))
		/* Calculate the function */
		if isFunctionInFormula {
			for x := 0; x < len(arrFormula); x++ {
				fltResult := 0.0
				isFunction := false
				theFormula := fmt.Sprintf("%v", arrFormula[x])
				theResult := fmt.Sprintf("%v", arrResult[x])
				isFunction, _ = checkFunction(theFormula)
				rawFormula := strings.ToUpper(theFormula)
				if isFunction {
					println("Check 1 theFormula: " + theFormula + ", theResult: " + theResult + fmt.Sprintf(", \nmapData: %+v", mapData))
					if strings.Contains(rawFormula, "SUM") {
						fltResult = modules.GetSUM(rawFormula, mapData)
					} else if strings.Contains(rawFormula, "AVG") {
						fltResult = modules.GetAVG(rawFormula, mapData)
					} else if strings.Contains(rawFormula, "COUNT") {
						fltResult = modules.GetCOUNT(rawFormula, mapData)
					} else if strings.Contains(rawFormula, "MAX") {
						fltResult = modules.GetMAX(rawFormula, mapData)
					} else if strings.Contains(rawFormula, "MIN") {
						fltResult = modules.GetMIN(rawFormula, mapData)
					} else {
						isFunction = false
					}
					for _, datum := range mapData {
						datum[theResult] = fltResult
					}
					println("Check 2 theFormula: " + theFormula + ", theResult: " + theResult + fmt.Sprintf(", \nmapData: %+v", mapData))
				}
			}
		}
		println("FINISHING DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapData: %+v", mapData))
		for _, datum := range mapData {
			/* save to Table Transaction/Result */
			dataId := modules.GetStringFromMapInterface(datum, "data_id")
			dataReceiveDateTime := modules.GetStringFromMapInterface(datum, "data_receive_datetime")
			if len(dataReceiveDateTime) < 1 {
				strReceiveDateTime = dataReceiveDateTime
			}
			delete(datum, "_id")
			delete(datum, "data_id")
			delete(datum, "formula_id")
			delete(datum, "client_id")
			delete(datum, "processid")
			delete(datum, "data_receive_datetime")
			println("FINISHING DATA 01 strClientID: " + incClientID + ", dataId: " + dataId + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \ndatum: %+v", datum))
			jsonData := modules.ConvertMapInterfaceToJSON(datum)
			processSavetoAnotherDB(dbPostgres, incTraceCode, dataId, incClientID, incFormulaID, incProcessID,
				jsonData, strReceiveDateTime)
		}
	}
}

// GetFormula /* func in test.go */
func GetFormula(dbPostgres *sql.DB, dbMongo *mongo.Database, rc *redis.Client, cx context.Context, incTraceCode string,
	incClientID string, incFormulaID string, redisKey string) {

	queryX := `SELECT client_id, formula_id FROM yformula_v3 WHERE client_id LIKE $1 AND formula_id LIKE $2`

	rowsX, errX := dbPostgres.Query(queryX, "%"+incClientID+"%", "%"+incFormulaID+"%")

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
				//println("Formula Ready strClientID: " + strClientID + ", strFormulaID: " + strFormulaID)
				GetData(dbPostgres, dbMongo, rc, cx, incTraceCode, strClientID, strFormulaID, redisKey)
			}
		}
	}

}
