package main

import (
	"billing/modules"
	"context"
	"database/sql"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"runtime"
	"strings"
	"time"
)

func getFormula(db *sql.DB, incFormula string) string {

	var arrField [20]string
	var arrColumn []string

	strFormula := ""

	query := "SELECT field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, " +
		"field11, field12, field13, field14, field15, field16, field17, field18, field19, field20," +
		"formula " +
		"FROM theformula WHERE formula_id = $1"

	rows, err := db.Query(query, incFormula)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		arrColumn, _ = rows.Columns()

		for rows.Next() {
			var rawField [20]sql.NullString
			var rawFormula sql.NullString

			errP := rows.Scan(&rawField[0], &rawField[1], &rawField[2], &rawField[3], &rawField[4], &rawField[5],
				&rawField[6], &rawField[7], &rawField[8], &rawField[9], &rawField[10], &rawField[11], &rawField[12],
				&rawField[13], &rawField[14], &rawField[15], &rawField[16], &rawField[17], &rawField[18], &rawField[19],
				&rawFormula)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				strFormula = modules.ConvertSQLNullStringToString(rawFormula)

				for x := 0; x < len(arrField); x++ {
					arrField[x] = modules.ConvertSQLNullStringToString(rawField[x])
				}
			}
		}
	}

	fmt.Println("Formula : ", strFormula)
	for x := 0; x < len(arrField)-1; x++ {
		if len(arrField[x]) > 0 {
			strFormula = strings.Replace(strFormula, arrField[x], arrColumn[x], -1)
		}
	}

	return strFormula
}

func getResult(incFormula string, mapData map[string]interface{}) interface{} {

	strFormula := getFormula(db, incFormula)
	expression, _ := govaluate.NewEvaluableExpression(strFormula)
	fmt.Println("Conversion : ", expression)
	result, _ := expression.Evaluate(mapData)

	return result
}

func getData(db *sql.DB) {

	var mapData = make(map[string]interface{})

	query := "SELECT formula_id, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, " +
		"field11, field12, field13, field14, field15, field16, field17, field18, field19, field20 " +
		"FROM thedata"

	rows, err := db.Query(query)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawFormula sql.NullString
			var rawField [20]sql.NullFloat64

			errP := rows.Scan(&rawFormula, &rawField[0], &rawField[1], &rawField[2], &rawField[3], &rawField[4], &rawField[5],
				&rawField[6], &rawField[7], &rawField[8], &rawField[9], &rawField[10], &rawField[11], &rawField[12],
				&rawField[13], &rawField[14], &rawField[15], &rawField[16], &rawField[17], &rawField[18], &rawField[19])

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				strFormula := modules.ConvertSQLNullStringToString(rawFormula)

				for x := 0; x < 20; x++ {
					mapData["field"+fmt.Sprintf("%v", x+1)] = modules.ConvertSQLNullFloat64ToFloat64(rawField[x])
				}

				result := getResult(strFormula, mapData)
				fmt.Println("RESULT ===> ", result)
				fmt.Println("")
			}
		}
	}
}

var db *sql.DB
var redisClient *redis.Client
var contextX context.Context

func main() {
	// Load configuration file
	modules.InitiateGlobalVariables(false)
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
	redisClient = modules.InitiateRedisClient()
	contextX = context.Background()
	errRedis := redisClient.Ping(contextX).Err()
	if errRedis != nil {
		panic(errRedis)
	} else {
		fmt.Println("Success connected to Redis")
	}

	getData(db)

}
