package main

import (
	"billing/Config"
	"billing/CoreApplication/Cron/CronProcessed"
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	guuid "github.com/google/uuid"
	"github.com/jinzhu/now"
	_ "github.com/lib/pq"
	"runtime"
	"strings"
	"time"
)

func loadToRedis(db *sql.DB, rc *redis.Client, cx context.Context) {

	var arrField [20]string
	var arrColumn []string

	query := "SELECT field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, " +
		"field11, field12, field13, field14, field15, field16, field17, field18, field19, field20," +
		"formula, client_id, formula_id, formula_type, formula_time " +
		"FROM formula WHERE formula_type = $1 AND is_active = true"

	rows, err := db.Query(query, processType)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		arrColumn, _ = rows.Columns()

		for rows.Next() {
			var rawField [20]sql.NullString
			var rawFormula sql.NullString
			var rawClientID sql.NullString
			var rawFormulaID sql.NullString
			var rawType sql.NullString
			var rawTime sql.NullTime

			errP := rows.Scan(&rawField[0], &rawField[1], &rawField[2], &rawField[3], &rawField[4], &rawField[5],
				&rawField[6], &rawField[7], &rawField[8], &rawField[9], &rawField[10], &rawField[11], &rawField[12],
				&rawField[13], &rawField[14], &rawField[15], &rawField[16], &rawField[17], &rawField[18], &rawField[19],
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

				for x := 0; x < len(arrField); x++ {
					arrField[x] = modules.ConvertSQLNullStringToString(rawField[x])
					if len(arrField[x]) > 0 {
						strFormula = strings.Replace(strFormula, arrField[x], arrColumn[x], -1)
					}
				}

				var mapRedis = make(map[string]interface{})
				mapRedis["client"] = strClientID
				mapRedis["formula"] = strFormula
				mapRedis["type"] = strType
				mapRedis["time"] = strTime
				jsonRedis := modules.ConvertMapInterfaceToJSON(mapRedis)

				redisKey := processType + "_" + Config.ConstRedisKey + strFormulaID
				errR := modules.RedisSet(rc, cx, redisKey, jsonRedis, 5*time.Second)
				if errR == nil {
					fmt.Println("Success load : ", strFormulaID)
				} else {
					fmt.Println("Failed to load : ", strFormulaID)
				}
			}
		}
	}
}

func readRedisFormula(rc *redis.Client, cx context.Context) {

	incTraceCode := modules.GenerateUUID()
	incTransactionID := guuid.New().String()
	refDayNow := modules.DoFormatDateTime("0D", time.Now())
	refDateNow := modules.DoFormatDateTime("YYYY-0M-0D", time.Now())
	refTimeNow := modules.DoFormatDateTime("HH:mm", time.Now())
	refDayName := time.Now().Weekday()
	refStartMonth := modules.DoFormatDateTime("YYYY-0M-0D", now.EndOfMonth())
	refEndMonth := modules.DoFormatDateTime("YYYY-0M-0D", now.BeginningOfMonth())

	redisKey := processType + "_" + Config.ConstRedisKey + "*"

	isValid, arrRedis := modules.RedisKeysByPattern(rc, cx, redisKey)
	if isValid {
		for x := 0; x < len(arrRedis); x++ {

			arrRedisID := strings.Split(arrRedis[x], "_")
			strFormulaID := arrRedisID[1]

			redisVal, errR := modules.RedisGet(rc, cx, arrRedis[x])
			if errR == nil {
				mapRedis := modules.ConvertJSONStringToMap("", redisVal)
				strClient := modules.GetStringFromMapInterface(mapRedis, "client")
				strFormula := modules.GetStringFromMapInterface(mapRedis, "formula")
				strType := modules.GetStringFromMapInterface(mapRedis, "type")
				strTime := modules.GetStringFromMapInterface(mapRedis, "time")

				if strings.ToUpper(strType) == "DAY" && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime, strFormula)
					CronProcessed.GetData(db, rc, cx, incTraceCode, incTransactionID, strClient, strFormulaID, strFormula)
				}

				if strings.ToUpper(fmt.Sprintf("%s", refDayName)) == strings.ToUpper(strType) && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime, strFormula)
					CronProcessed.GetData(db, rc, cx, incTraceCode, incTransactionID, strClient, strFormulaID, strFormula)
				}

				if strings.ToUpper(strType) == "STARTMONTH" && refStartMonth == refDateNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime, strFormula)
					CronProcessed.GetData(db, rc, cx, incTraceCode, incTransactionID, strClient, strFormulaID, strFormula)
				}

				if strings.ToUpper(strType) == "ENDMONTH" && refEndMonth == refDateNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime, strFormula)
					CronProcessed.GetData(db, rc, cx, incTraceCode, incTransactionID, strClient, strFormulaID, strFormula)
				}

				if strings.ToUpper(strType) == refDayNow && refTimeNow == strTime {
					fmt.Println(strFormulaID, strType, strTime, strFormula)
					CronProcessed.GetData(db, rc, cx, incTraceCode, incTransactionID, strClient, strFormulaID, strFormula)
				}

				if strings.ToUpper(strType) == "REALTIME" {
					fmt.Println(strFormulaID, strType, strTime, strFormula)
					CronProcessed.GetData(db, rc, cx, incTraceCode, incTransactionID, strClient, strFormulaID, strFormula)
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
