package main

import (
	"billing/Config"
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func getFormula() {
	//strFormula := "@formula: hasil1 = hargabarang * jumlahbarang@" +
	//	"@formula: hasil2 = SUM(hasil1)@" +
	//	"@formula: hasil3 = hasil2 * 10%@" +
	//	"@formula: hasil4 = hasil3 + biayaadmin@"

	strFormula := "@string: nama@" +
		"@string: cabang@" +
		"@string: barang@" +
		"@formula: total = jumlah * harga@" +
		"@formula: fee  = total * 10 / 100@" +
		"@formula: totalpenjualan = SUM(total)@" +
		"@formula: totalfee = SUM(fee)@" +
		"@outputheader: nama, cabang, barang, jumlah, harga, total, fee@" +
		"@outputdatagroup: nama asc, cabang asc, barang asc@" +
		"@outputrecapgroup: totalpenjualan@" +
		"@outputrecapgroup: totalfee@"

	var strInsertAsResult []string
	var strInsertAsFormula []string

	var strInsertAsResultString []string
	var strInsertAsFormulaString []string

	var strOutputHeader []string

	var strOutputDataGroup []string
	var strOutputRecapGroup []string

	arrFormulas := strings.Split(strFormula, "@")
	for x := 0; x < len(arrFormulas); x++ {
		arrContent := strings.Split(arrFormulas[x], ":")

		if len(arrFormulas[x]) > 0 {
			incID := arrContent[0]
			incParameter := strings.TrimLeft(strings.TrimRight(arrContent[1], " "), " ")

			if strings.ToUpper(incID) == "STRING" {
				incParameter = incParameter + " = " + incParameter
				rawParameter := strings.Split(incParameter, "=")

				for y := 0; y < len(rawParameter); y++ {

					if y == 0 {
						rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
						strInsertAsResultString = append(strInsertAsResultString, rawResults)
					}
					if y == 1 {
						rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[1], " "), " ")
						strInsertAsFormulaString = append(strInsertAsFormulaString, rawFormulas)
					}
				}

			} else if strings.ToUpper(incID) == "FORMULA" {
				fmt.Println(incParameter)
				rawParameter := strings.Split(incParameter, "=")

				for y := 0; y < len(rawParameter); y++ {

					if y == 0 {
						rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
						strInsertAsResult = append(strInsertAsResult, rawResults)
					}
					if y == 1 {
						rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[1], " "), " ")
						strInsertAsFormula = append(strInsertAsFormula, rawFormulas)
					}
				}

			} else if strings.ToUpper(incID) == "OUTPUTHEADER" {
				fmt.Println(incParameter)
				rawParameter := strings.Split(incParameter, "=")

				for y := 0; y < len(rawParameter); y++ {

					if y == 0 {
						rawHeader := strings.Split(rawParameter[0], ",")

						for z := 0; z < len(rawHeader); z++ {
							rawHeaders := strings.TrimLeft(strings.TrimRight(rawHeader[z], " "), " ")
							strOutputHeader = append(strOutputHeader, rawHeaders)
						}
					}
				}

			} else if strings.ToUpper(incID) == "OUTPUTDATAGROUP" {
				fmt.Println(incParameter)
				rawParameter := strings.Split(incParameter, "=")

				for y := 0; y < len(rawParameter); y++ {

					if y == 0 {
						rawGroup := strings.Split(rawParameter[0], ",")

						for z := 0; z < len(rawGroup); z++ {
							rawGroups := strings.TrimLeft(strings.TrimRight(rawGroup[0], " "), " ")
							strOutputDataGroup = append(strOutputDataGroup, rawGroups)
						}
					}
				}

			} else if strings.ToUpper(incID) == "OUTPUTRECAPGROUP" {
				fmt.Println(incParameter)
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

	fmt.Println(len(strInsertAsResult), strInsertAsResult)
	fmt.Println(len(strInsertAsFormula), strInsertAsFormula)

	fmt.Println(len(strInsertAsResultString), strInsertAsResultString)
	fmt.Println(len(strInsertAsFormulaString), strInsertAsFormulaString)

	fmt.Println(len(strOutputHeader), strOutputHeader)

	fmt.Println(len(strOutputDataGroup), strOutputDataGroup)
	fmt.Println(len(strOutputRecapGroup), strOutputRecapGroup)

}

func getField() {
	strFormula := "@formula: hasil1 = hargabarang * jumlahbarang@" +
		"@formula: hasil2 = SUM(hasil1)@" +
		"@formula: hasil3 = hasil2 * 10%@" +
		"@formula: hasil4 = hasil3 + biayaadmin@"

	var strInsertAsField []string
	var strInsertAsFormula []string

	arrFormula := strings.Split(strFormula, "@")
	for x := 0; x < len(arrFormula); x++ {
		//fmt.Println(x, " => ", arrFormula[x])
		arrHead := strings.Split(arrFormula[x], ":")

		if x%2 == 1 {
			for y := 0; y < len(arrHead); y++ {
				//fmt.Println(y, " => ", arrHead[y])
				arrContent := strings.Split(arrHead[y], "=")

				for z := 0; z < len(arrContent); z++ {
					if !strings.Contains(arrContent[z], "formula") {
						//fmt.Println(z, " => ", strings.TrimLeft(arrContent[z], " "))

						if z == 0 {
							strInsertAsField = append(strInsertAsField, arrContent[z])
						}
						if z == 1 {
							strInsertAsFormula = append(strInsertAsFormula, arrContent[z])
						}
					}
				}
			}
		}
	}

	fmt.Println(len(strInsertAsField), strInsertAsField)
	fmt.Println(len(strInsertAsFormula), strInsertAsFormula)

}

func ReloadFormulaToRedis(db *sql.DB, rc *redis.Client, cx context.Context) {

	var arrF [50]string
	var arrC []string

	query := "SELECT f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, " +
		"f11, f12, f13, f14, f15, f16, f17, f18, f19, f20," +
		"f21, f22, f23, f24, f25, f26, f27, f28, f29, f30," +
		"f31, f32, f33, f34, f35, f36, f37, f38, f39, f40," +
		"f41, f42, f43, f44, f45, f46, f47, f48, f49, f50," +
		"formula, client_id, formula_id, formula_type, formula_time " +
		"FROM yformula WHERE is_active = true"

	rows, err := db.Query(query)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		arrC, _ = rows.Columns()

		for rows.Next() {
			var rawF [50]sql.NullString
			var rawFormula sql.NullString
			var rawClientID sql.NullString
			var rawFormulaID sql.NullString
			var rawType sql.NullString
			var rawTime sql.NullTime

			errP := rows.Scan(&rawF[0], &rawF[1], &rawF[2], &rawF[3], &rawF[4], &rawF[5], &rawF[6], &rawF[7], &rawF[8], &rawF[9], &rawF[10],
				&rawF[11], &rawF[12], &rawF[13], &rawF[14], &rawF[15], &rawF[16], &rawF[17], &rawF[18], &rawF[19], &rawF[20],
				&rawF[21], &rawF[22], &rawF[23], &rawF[24], &rawF[25], &rawF[26], &rawF[27], &rawF[28], &rawF[29], &rawF[30],
				&rawF[31], &rawF[32], &rawF[33], &rawF[34], &rawF[35], &rawF[36], &rawF[37], &rawF[38], &rawF[39], &rawF[40],
				&rawF[41], &rawF[42], &rawF[43], &rawF[44], &rawF[45], &rawF[46], &rawF[47], &rawF[48], &rawF[49],
				&rawFormula, &rawClientID, &rawFormulaID, &rawType, &rawTime)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				strFormula := ""
				strFormula = modules.ConvertSQLNullStringToString(rawFormula)
				strClientID := modules.ConvertSQLNullStringToString(rawClientID)
				strFormulaID := modules.ConvertSQLNullStringToString(rawFormulaID)
				strType := modules.ConvertSQLNullStringToString(rawType)
				strTime := ""
				if rawTime.Valid {
					strTime = modules.DoFormatDateTime("HH:mm", rawTime.Time)
				}

				//for x := 0; x < len(arrF); x++ {
				for x := len(arrF) - 1; x >= 0; x-- {
					arrF[x] = modules.ConvertSQLNullStringToString(rawF[x])
					if len(arrF[x]) > 0 {
						//strfmt := fmt.Sprintf("%s[^a-zA-Z0-9)]", arrF[x])
						//println("strfmt: " + strfmt)
						//reg := regexp.MustCompile(strfmt)
						//strFormula = reg.ReplaceAllString(strFormula, arrC[x])
						strFormula = strings.Replace(strFormula, arrF[x], arrC[x], -1)
					}
				}
				println("strFormula: " + strFormula)

				var strInsertAsResult []string
				var strInsertAsFormula []string

				var strInsertAsResultString []string

				var strOutputHeader []string

				var strOutputDataGroup []string
				var strOutputRecapGroup []string

				arrFormulas := strings.Split(strFormula, "@")
				for x := 0; x < len(arrFormulas); x++ {
					arrContent := strings.Split(arrFormulas[x], ":")

					if len(arrFormulas[x]) > 1 && len(arrContent) > 1 {

						incID := arrContent[0]
						incParameter := strings.TrimLeft(strings.TrimRight(arrContent[1], " "), " ")

						if strings.ToUpper(incID) == "STRING" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strInsertAsResultString = append(strInsertAsResultString, rawResults)
								}
							}

						} else if strings.ToUpper(incID) == "FORMULA" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strInsertAsResult = append(strInsertAsResult, rawResults)
								}
								if y == 1 {
									rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[1], " "), " ")
									strInsertAsFormula = append(strInsertAsFormula, rawFormulas)
								}
							}

						} else if strings.ToUpper(incID) == "OUTPUTHEADER" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawHeader := strings.Split(rawParameter[0], ",")

									for z := 0; z < len(rawHeader); z++ {
										rawHeaders := strings.TrimLeft(strings.TrimRight(rawHeader[z], " "), " ")
										strOutputHeader = append(strOutputHeader, rawHeaders)
									}
								}
							}

						} else if strings.ToUpper(incID) == "OUTPUTDATAGROUP" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawGroup := strings.Split(rawParameter[0], ",")

									for z := 0; z < len(rawGroup); z++ {
										rawGroups := strings.TrimLeft(strings.TrimRight(rawGroup[z], " "), " ")
										strOutputDataGroup = append(strOutputDataGroup, rawGroups)
									}
								}
							}

						} else if strings.ToUpper(incID) == "OUTPUTRECAPGROUP" {
							fmt.Println(incParameter)
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

				var mapRedis = make(map[string]interface{})
				mapRedis["client"] = strClientID
				mapRedis["formula"] = strInsertAsFormula
				mapRedis["result"] = strInsertAsResult
				mapRedis["string"] = strInsertAsResultString
				mapRedis["header"] = strOutputHeader
				mapRedis["data"] = strOutputDataGroup
				mapRedis["recap"] = strOutputRecapGroup
				mapRedis["type"] = strType
				mapRedis["time"] = strTime
				jsonRedis := modules.ConvertMapInterfaceToJSON(mapRedis)

				redisKey := "test_" + strFormulaID
				errR := modules.RedisSet(rc, cx, redisKey, jsonRedis, 0)
				if errR == nil {
					//fmt.Println("Success load : ", strFormulaID)
				} else {
					fmt.Println("Failed to load : ", strFormulaID)
				}
			}
		}
	}
}

func ReloadFormulaToRedisV2(db *sql.DB, rc *redis.Client, cx context.Context) {

	query := `SELECT formula, client_id, formula_id, formula_type, formula_time FROM yformula_v3
        WHERE is_active = true`

	rows, err := db.Query(query)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawFormula sql.NullString
			var rawClientID sql.NullString
			var rawFormulaID sql.NullString
			var rawType sql.NullString
			var rawTime sql.NullTime

			errP := rows.Scan(&rawFormula, &rawClientID, &rawFormulaID, &rawType, &rawTime)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				//strFields := modules.ConvertSQLNullStringToString(rawFields)
				//mapFields := modules.ConvertJSONStringToMap("", strFields)
				strFormula := ""
				strFormula = modules.ConvertSQLNullStringToString(rawFormula)
				strClientID := modules.ConvertSQLNullStringToString(rawClientID)
				strFormulaID := modules.ConvertSQLNullStringToString(rawFormulaID)
				strType := modules.ConvertSQLNullStringToString(rawType)
				strTime := ""
				if rawTime.Valid {
					strTime = modules.DoFormatDateTime("HH:mm", rawTime.Time)
				}
				println("strFormula: " + strFormula)

				var strInsertAsResult []string
				var strInsertAsFormula []string

				var strInsertAsResultString []string

				var strOutputHeader []string

				var strOutputDataGroup []string
				var strOutputRecapGroup []string

				arrFormulas := strings.Split(strFormula, "@")
				for x := 0; x < len(arrFormulas); x++ {
					arrContent := strings.Split(arrFormulas[x], ":")

					if len(arrFormulas[x]) > 1 && len(arrContent) > 1 {

						incID := arrContent[0]
						incParameter := strings.TrimLeft(strings.TrimRight(arrContent[1], " "), " ")

						if strings.ToUpper(incID) == "STRING" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strInsertAsResultString = append(strInsertAsResultString, rawResults)
								}
							}

						} else if strings.ToUpper(incID) == "FORMULA" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
									strInsertAsResult = append(strInsertAsResult, rawResults)
								}
								if y == 1 {
									rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[1], " "), " ")
									strInsertAsFormula = append(strInsertAsFormula, rawFormulas)
								}
							}

						} else if strings.ToUpper(incID) == "OUTPUTHEADER" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawHeader := strings.Split(rawParameter[0], ",")

									for z := 0; z < len(rawHeader); z++ {
										rawHeaders := strings.TrimLeft(strings.TrimRight(rawHeader[z], " "), " ")
										strOutputHeader = append(strOutputHeader, rawHeaders)
									}
								}
							}

						} else if strings.ToUpper(incID) == "OUTPUTDATAGROUP" {
							fmt.Println(incParameter)
							rawParameter := strings.Split(incParameter, "=")

							for y := 0; y < len(rawParameter); y++ {

								if y == 0 {
									rawGroup := strings.Split(rawParameter[0], ",")

									for z := 0; z < len(rawGroup); z++ {
										rawGroups := strings.TrimLeft(strings.TrimRight(rawGroup[z], " "), " ")
										strOutputDataGroup = append(strOutputDataGroup, rawGroups)
									}
								}
							}

						} else if strings.ToUpper(incID) == "OUTPUTRECAPGROUP" {
							fmt.Println(incParameter)
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

				var mapRedis = make(map[string]interface{})
				mapRedis["client"] = strClientID
				mapRedis["formula"] = strInsertAsFormula
				mapRedis["result"] = strInsertAsResult
				mapRedis["string"] = strInsertAsResultString
				mapRedis["header"] = strOutputHeader
				mapRedis["data"] = strOutputDataGroup
				mapRedis["recap"] = strOutputRecapGroup
				mapRedis["type"] = strType
				mapRedis["time"] = strTime
				jsonRedis := modules.ConvertMapInterfaceToJSON(mapRedis)

				redisKey := "test_" + strFormulaID
				errR := modules.RedisSet(rc, cx, redisKey, jsonRedis, 0)
				if errR == nil {
					//fmt.Println("Success load : ", strFormulaID)
				} else {
					fmt.Println("Failed to load : ", strFormulaID)
				}
			}
		}
	}
}

func GetFormula(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incClientID string, incFormulaID string) {

	queryX := `SELECT client_id, formula_id FROM yformula_v3 WHERE client_id LIKE $1 AND formula_id LIKE $2`

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
				println("Formula Ready strClientID: " + strClientID + ", strFormulaID: " + strFormulaID)
				GetDataV2(db, rc, cx, incTraceCode, strClientID, strFormulaID)
			}
		}
	}

}

func GetData(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incClientID string, incFormulaID string) {

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

				redisKey := "test_" + strFormulaID
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

func GetDataV2(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incClientID string, incFormulaID string) {

	var mapDatas []map[string]interface{}
	incDataID := ""
	isFunctionInFormula := false
	strReceiveDateTime := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	redisKey := "test_" + incFormulaID
	redisVal, _ := modules.RedisGet(rc, cx, redisKey)
	mapRedis := modules.ConvertJSONStringToMap("", redisVal)

	arrFormula := mapRedis["formula"].([]interface{})
	arrResult := mapRedis["result"].([]interface{})

	incProcessID := modules.GenerateUUID()

	queryX := `SELECT datas, data_id, data_receive_datetime FROM ydata_v2 
        WHERE is_process = false AND client_id = $1 AND formula_id = $2`
	println("Before Query DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID)

	rowsX, errX := db.Query(queryX, incClientID, incFormulaID)

	if errX != nil {
		fmt.Println("FAILED : ", errX)
	} else {
		println("Before Next DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID)
		for rowsX.Next() {
			println("Before Scan DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID)
			//var rawClientID sql.NullString
			//var rawFormulaID sql.NullString
			var rawDatas sql.NullString
			var rawDataID sql.NullString
			var rawReceiveDateTime sql.NullString

			errPX := rowsX.Scan(&rawDatas, &rawDataID, &rawReceiveDateTime)

			if errPX != nil {
				fmt.Println("FAILED : ", errPX)
			} else {
				//var mapResults = make(map[string]string)
				//strClientID := modules.ConvertSQLNullStringToString(rawClientID)
				//strFormulaID := modules.ConvertSQLNullStringToString(rawFormulaID)
				incDataID = modules.ConvertSQLNullStringToString(rawDataID)
				strDatas := modules.ConvertSQLNullStringToString(rawDatas)
				strReceiveDateTime = modules.ConvertSQLNullStringToString(rawReceiveDateTime)
				mapData := modules.ConvertJSONStringToMap("", strDatas)
				println("Ready DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapData %+v", mapData))
				for key, value := range mapData {
					strF := value.(string)
					fltF, errParse := strconv.ParseFloat(strF, 64)
					if errParse == nil {
						mapData[key] = fltF
					} else {
						mapData[key] = strF
					}
				}

				//strType := modules.GetStringFromMapInterface(mapRedis, "type")
				//strTime := modules.GetStringFromMapInterface(mapRedis, "time")

				//var strResult string

				for x := 0; x < len(arrFormula); x++ {
					//isSuccess := false
					isFunctionSub := false
					strResult := ""
					theFormula := fmt.Sprintf("%v", arrFormula[x])
					theResult := fmt.Sprintf("%v", arrResult[x])

					mapData["dataid"] = incDataID
					mapData["processid"] = incProcessID

					_, isFunctionSub, strResult = processForNonStringV2(db, rc, cx, theFormula, mapData)
					if isFunctionSub {
						mapData[theResult] = "0"
						isFunctionInFormula = isFunctionSub
					} else {
						fltF, errParse := strconv.ParseFloat(strResult, 64)
						if errParse == nil {
							mapData[theResult] = fltF
						} else {
							mapData[theResult] = strResult
						}
						//mapData[theResult] = fltF
					}
				}
				// Bila akan di kumpulkan datanya di table lain
				//_, _ = processSavetoAnotherDB(db, rc, cx, incTraceCode, incDataID)
				mapDatas = append(mapDatas, mapData)
			}
		}
		println("MID DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapDatas: %+v", mapDatas))
		/* Calculate the function */
		if isFunctionInFormula {
			for x := 0; x < len(arrFormula); x++ {
				fltResult := 0.0
				isFunction := false
				theFormula := fmt.Sprintf("%v", arrFormula[x])
				theResult := fmt.Sprintf("%v", arrResult[x])
				isFunction, _ = checkFunctionV2(db, theFormula, incProcessID)
				rawFormula := strings.ToUpper(theFormula)
				if isFunction {
					println("Check 1 theFormula: " + theFormula + ", theResult: " + theResult + fmt.Sprintf(", \nmapDatas: %+v", mapDatas))
					if strings.Contains(rawFormula, "SUM") {
						fltResult = getSUMV2(db, rawFormula, mapDatas)
					} else if strings.Contains(rawFormula, "AVG") {
						fltResult = getAVGV2(db, rawFormula, mapDatas)
					} else if strings.Contains(rawFormula, "COUNT") {
						fltResult = getCOUNTV2(db, rawFormula, mapDatas)
					} else if strings.Contains(rawFormula, "MAX") {
						fltResult = getMAXV2(db, rawFormula, mapDatas)
					} else if strings.Contains(rawFormula, "MIN") {
						fltResult = getMINV2(db, rawFormula, mapDatas)
					} else {
						isFunction = false
					}
					for _, mapData := range mapDatas {
						mapData[theResult] = fltResult
					}
					println("Check 2 theFormula: " + theFormula + ", theResult: " + theResult + fmt.Sprintf(", \nmapDatas: %+v", mapDatas))
				}
			}
		}
		println("FINISHING DATA strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapDatas: %+v", mapDatas))
		for _, mapData := range mapDatas {
			/* save to Table Transaction/Result */
			println("FINISHING DATA 01 strClientID: " + incClientID + ", strFormulaID: " + incFormulaID + fmt.Sprintf(", \nmapData: %+v", mapData))
			jsonData := modules.ConvertMapInterfaceToJSON(mapData)
			processSavetoAnotherDBV2(db, rc, cx, incTraceCode, mapData["dataid"].(string), incClientID, incFormulaID, incProcessID,
				jsonData, strReceiveDateTime)
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

func processSavetoAnotherDBV2(db *sql.DB, rc *redis.Client, cx context.Context, incTraceCode string, incDataID string,
	incClientID string, incFormulaID string, incProcessID string, jsonDatas string, incTimeReceive string) (bool, string) {
	incTimeNow := modules.DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	query := `INSERT INTO ytransaction_v2 (data_id, client_id, formula_id, process_id, results, data_receive_datetime, 
        data_process_datetime, data_receive_code, is_process) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`

	result, err := db.Exec(query, incDataID, incClientID, incFormulaID, incProcessID, jsonDatas, incTimeReceive, incTimeNow,
		incTraceCode, false)

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

func getSUMV2(db *sql.DB, incFormula string, mapDatas []map[string]interface{}) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "SUM", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	for _, mapData := range mapDatas {
		fltResult += mapData[rawFormula].(float64)
	}

	return fltResult
}

func getAVGV2(db *sql.DB, incFormula string, mapDatas []map[string]interface{}) float64 {
	fltResult := 0.0
	fltTotal := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "AVG", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	for _, mapData := range mapDatas {
		fltResult += mapData[rawFormula].(float64)
		fltTotal += 1
	}
	fltResult = fltResult / fltTotal

	return fltResult
}

func getCOUNTV2(db *sql.DB, incFormula string, mapDatas []map[string]interface{}) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "COUNT", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	for _, mapData := range mapDatas {
		if mapData[rawFormula].(float64) > 0 {
			fltResult += 1
		}
	}

	return fltResult
}

func getMAXV2(db *sql.DB, incFormula string, mapDatas []map[string]interface{}) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "MAX", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	max := 0.0
	for _, mapData := range mapDatas {
		if mapData[rawFormula].(float64) > max {
			max = mapData[rawFormula].(float64)
		}
	}

	return fltResult
}

func getMINV2(db *sql.DB, incFormula string, mapDatas []map[string]interface{}) float64 {
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	rawFormula = strings.Replace(rawFormula, "MIN", "", -1)
	rawFormula = strings.Replace(rawFormula, "(", "", -1)
	rawFormula = strings.Replace(rawFormula, ")", "", -1)
	rawFormula = strings.ToLower(rawFormula)

	min := 0.0
	for _, mapData := range mapDatas {
		if mapData[rawFormula].(float64) < min {
			min = mapData[rawFormula].(float64)
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

func checkFunctionV2(db *sql.DB, incFormula string, incProcessID string) (bool, interface{}) {
	isValid := true
	fltResult := 0.0

	rawFormula := strings.ToUpper(incFormula)

	if strings.Contains(rawFormula, "SUM") {
		//fltResult = getSUM(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "AVG") {
		//fltResult = getAVG(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "COUNT") {
		//fltResult = getCOUNT(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "MAX") {
		//fltResult = getMAX(db, incFormula, incProcessID)
	} else if strings.Contains(rawFormula, "MIN") {
		//fltResult = getMIN(db, incFormula, incProcessID)
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
		println(fmt.Sprintf("expression: %+v", expression))
		//fmt.Println(expression)
		println(fmt.Sprintf("mapData: %+v", mapData))
		rawResult, err := expression.Evaluate(mapData)
		println(fmt.Sprintf("rawResult: %+v", rawResult))
		incResult = fmt.Sprintf("%v", rawResult)
		println(fmt.Sprintf("incResult: %+v", incResult))

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

func processForNonStringV2(db *sql.DB, rc *redis.Client, cx context.Context, incFormula string, mapData map[string]interface{}) (bool, bool, string) {

	strResult := ""
	isSuccess := false
	//incDataID := modules.GetStringFromMapInterface(mapData, "dataid")
	incProcessID := modules.GetStringFromMapInterface(mapData, "processid")

	isFunction, _ := checkFunctionV2(db, incFormula, incProcessID)
	if !isFunction {
		expression, _ := govaluate.NewEvaluableExpression(incFormula)
		println(fmt.Sprintf("expression: %+v", expression))
		//fmt.Println(expression)
		println(fmt.Sprintf("mapData: %+v", mapData))
		rawResult, err := expression.Evaluate(mapData)
		strResult = fmt.Sprintf("%v", rawResult)
		println(fmt.Sprintf("strResult: %+v", strResult))

		if err == nil {
			//updateDatabase(db, "incTraceCode", incField, incResult, incDataID, incProcessID, incClientID)
			//updateDatabase(db, "", incField, incResult, incDataID, incClientID, incProcessID)
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
		//updateDatabase(db, "incTraceCode", incField, incResult, incDataID, incProcessID, incClientID)

	}
	fmt.Println("Result : ", strResult)

	return isSuccess, isFunction, strResult
}

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

func getPreviewData(db *sql.DB, rc *redis.Client, cx context.Context) {

	//query := "SELECT "

}

var db *sql.DB
var rc *redis.Client
var cx context.Context

func main() {
	// Load configuration file
	modules.InitiateGlobalVariables(Config.ConstProduction)
	runtime.GOMAXPROCS(4)

	// Initiate Database
	var errDB error
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		modules.MapConfig["databaseHost"], modules.MapConfig["databasePort"], modules.MapConfig["databaseUser"],
		modules.MapConfig["databasePass"], modules.MapConfig["databaseName"])

	db, errDB = sql.Open("postgres", psqlInfo) // db udah di defined diatas, jadi harus pake = bukan :=

	if errDB != nil {
		modules.DoLog("INFO", "", "ProfileGRPCServer", "main",
			"Failed to connect to database server. Error!", true, errDB)
		panic(errDB)
	}

	db.SetConnMaxLifetime(time.Minute * 10)
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(50)

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Failed to close DB Connection.")
		}
	}(db)

	errDB = db.Ping()
	if errDB != nil {
		panic(errDB)
	}

	//Initiate Redis
	rc = modules.InitiateRedisClient()
	cx = context.Background()
	errRedis := rc.Ping(cx).Err()
	if errRedis != nil {
		panic(errRedis)
	} else {
		fmt.Println("Success connected to Redis")
	}

	//getFormula2()

	//getFormula()

	//ReloadFormulaToRedis(db, rc, cx)
	//GetData(db, rc, cx, "incTraceCode", "", "")

	ReloadFormulaToRedisV2(db, rc, cx)
	GetFormula(db, rc, cx, "incTraceCode", "", "")

}
