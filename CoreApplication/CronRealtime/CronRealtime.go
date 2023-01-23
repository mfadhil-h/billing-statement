package main

import (
	"billing/Config"
	"billing/CoreApplication/CronRealtime/CronProcessed"
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/now"
	_ "github.com/lib/pq"
	"runtime"
	"strings"
	"time"
)

func loadToRedis(db *sql.DB, rc *redis.Client, cx context.Context) {

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
						strFormula = strings.Replace(strFormula, arrF[x], arrC[x], -1)
					}
				}

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

				redisKey := processType + "_" + Config.ConstRedisKey + strFormulaID
				//redisKey := "test_" + strFormulaID
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

func readRedisFormula(rc *redis.Client, cx context.Context) {

	incTraceCode := modules.GenerateUUID()
	refDayNow := modules.DoFormatDateTime("0D", time.Now())
	refDateNow := modules.DoFormatDateTime("YYYY-0M-0D", time.Now())
	refTimeNow := modules.DoFormatDateTime("HH:mm", time.Now())
	refDayName := time.Now().Weekday()
	refStartMonth := modules.DoFormatDateTime("YYYY-0M-0D", now.EndOfMonth())
	refEndMonth := modules.DoFormatDateTime("YYYY-0M-0D", now.BeginningOfMonth())

	redisKey := processType + "_" + Config.ConstRedisKey + "*"
	modules.DoLog("INFO", incTraceCode, "CRONREALTIME", "readRedisFormula",
		fmt.Sprintf("redisKey: %s", redisKey), false, nil)

	isValid, arrRedis := modules.RedisKeysByPattern(rc, cx, redisKey)
	if isValid {
		modules.DoLog("INFO", incTraceCode, "CRONREALTIME", "readRedisFormula",
			fmt.Sprintf("arrRedis: %+v", arrRedis), false, nil)
		for x := 0; x < len(arrRedis); x++ {
			modules.DoLog("INFO", incTraceCode, "CRONREALTIME", "readRedisFormula",
				fmt.Sprintf("arrRedis[x]: %s", arrRedis[x]), false, nil)
			arrRedisID := strings.Split(arrRedis[x], "_")
			modules.DoLog("INFO", incTraceCode, "CRONREALTIME", "readRedisFormula",
				fmt.Sprintf("arrRedisID: %+v", arrRedisID), false, nil)
			strFormulaID := arrRedisID[2]

			redisVal, errR := modules.RedisGet(rc, cx, arrRedis[x])
			if errR == nil {
				modules.DoLog("INFO", incTraceCode, "CRONREALTIME", "readRedisFormula",
					fmt.Sprintf("redisVal: %+v", redisVal), false, nil)
				mapRedis := modules.ConvertJSONStringToMap("", redisVal)
				modules.DoLog("INFO", incTraceCode, "CRONREALTIME", "readRedisFormula",
					fmt.Sprintf("mapRedis: %+v", mapRedis), false, nil)
				strClient := modules.GetStringFromMapInterface(mapRedis, "client")
				//strFormula := modules.GetStringFromMapInterface(mapRedis, "formula") // failed cuz its array
				strType := modules.GetStringFromMapInterface(mapRedis, "type")
				strTime := modules.GetStringFromMapInterface(mapRedis, "time")

				if strings.ToUpper(strType) == "DAY" && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					CronProcessed.GetData(db, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(fmt.Sprintf("%s", refDayName)) == strings.ToUpper(strType) && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					CronProcessed.GetData(db, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == "STARTMONTH" && refStartMonth == refDateNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					CronProcessed.GetData(db, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == "ENDMONTH" && refEndMonth == refDateNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					CronProcessed.GetData(db, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == refDayNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					CronProcessed.GetData(db, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == "REALTIME" {
					fmt.Println(strFormulaID, strType, strTime)
					CronProcessed.GetData(db, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}
			}
		}
	}
}

const processType = "REALTIME"

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

	// Initiate Redis
	rc = modules.InitiateRedisClient()
	cx = context.Background()
	errRedis := rc.Ping(cx).Err()
	if errRedis != nil {
		panic(errRedis)
	} else {
		fmt.Println("Success connected to Redis")
	}

	go func() {
		for {
			loadToRedis(db, rc, cx)
			time.Sleep(3 * time.Second)
		}
	}()

	loc, _ := time.LoadLocation("Asia/Jakarta")
	s := gocron.NewScheduler(loc)
	_, _ = s.Every(Config.ConstProcessDataRealtime).Do(readRedisFormula, rc, cx)
	s.StartBlocking()

}
