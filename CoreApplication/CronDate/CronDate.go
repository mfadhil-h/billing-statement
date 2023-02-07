package main

import (
	"billing/Config"
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/now"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"runtime"
	"strings"
	"time"
)

/* ReloadFormulaToRedis func in test.go */
func loadToRedis(db *sql.DB, rc *redis.Client, cx context.Context) {

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

/* Not to change if test.go change */
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
					/* Get Formula func in test.go (Originally GetData func) */
					modules.GetFormula(dbPostgres, dbMongo, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(fmt.Sprintf("%s", refDayName)) == strings.ToUpper(strType) && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					/* Get Formula func in test.go (Originally GetData func) */
					modules.GetFormula(dbPostgres, dbMongo, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == "STARTMONTH" && refStartMonth == refDateNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					/* Get Formula func in test.go (Originally GetData func) */
					modules.GetFormula(dbPostgres, dbMongo, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == "ENDMONTH" && refEndMonth == refDateNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					/* Get Formula func in test.go (Originally GetData func) */
					modules.GetFormula(dbPostgres, dbMongo, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == refDayNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime)
					/* Get Formula func in test.go (Originally GetData func) */
					modules.GetFormula(dbPostgres, dbMongo, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}

				if strings.ToUpper(strType) == "REALTIME" {
					fmt.Println(strFormulaID, strType, strTime)
					/* Get Formula func in test.go (Originally GetData func) */
					modules.GetFormula(dbPostgres, dbMongo, rc, cx, incTraceCode, strClient, strFormulaID, arrRedis[x])
				}
			}
		}
	}
}

const processType = "DATE"

var dbPostgres *sql.DB
var dbMongo *mongo.Database
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

	dbPostgres, errDB = sql.Open("postgres", psqlInfo) // dbPostgres udah di defined diatas, jadi harus pake = bukan :=

	if errDB != nil {
		modules.DoLog("INFO", "", "ProfileGRPCServer", "main",
			"Failed to connect to database server. Error!", true, errDB)
		panic(errDB)
	}

	dbPostgres.SetConnMaxLifetime(time.Minute * 10)
	dbPostgres.SetMaxIdleConns(5)
	dbPostgres.SetMaxOpenConns(50)

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Failed to close DB Connection.")
		}
	}(dbPostgres)

	errDB = dbPostgres.Ping()
	if errDB != nil {
		panic(errDB)
	}

	client, errDB := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if errDB != nil {
		panic(errDB)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	errDB = client.Connect(ctx)
	if errDB != nil {
		panic(errDB)
	}
	defer client.Disconnect(ctx)
	errDB = client.Ping(ctx, readpref.Primary())
	if errDB != nil {
		panic(errDB)
	}

	dbMongo = client.Database("billing_settlement")

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
			loadToRedis(dbPostgres, rc, cx)
			time.Sleep(Config.ConstReloadToRedis * time.Second)
		}
	}()

	loc, _ := time.LoadLocation("Asia/Jakarta")
	s := gocron.NewScheduler(loc)
	_, _ = s.Every(Config.ConstProcessDataDate).Do(readRedisFormula, rc, cx)
	s.StartBlocking()

}
