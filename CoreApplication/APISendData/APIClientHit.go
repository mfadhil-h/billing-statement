package main

import (
	"billing/Config"
	"billing/CoreApplication/APISendData/APISendData"
	"billing/modules"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"
)

var db *sql.DB
var rc *redis.Client
var cx context.Context

func loadCredentialToRedis(db *sql.DB, rc *redis.Client, cx context.Context) {

	redisDelPattern := Config.ConstRedisAPIHitKey + "*"
	modules.RedisDeleteKeysByPattern(rc, cx, redisDelPattern)

	query := "SELECT client_id, api_username, api_password, api_key, api_remote_ip_address " +
		"FROM user_api WHERE is_active = true"

	rows, err := db.Query(query)

	if err != nil {
		fmt.Println("FAILED : ", err)
	} else {
		for rows.Next() {
			var rawClientID sql.NullString
			var rawUsername sql.NullString
			var rawPassword sql.NullString
			var rawKey sql.NullString
			var rawRemoteIP sql.NullString

			errP := rows.Scan(&rawClientID, &rawUsername, &rawPassword, &rawKey, &rawRemoteIP)

			if errP != nil {
				fmt.Println("FAILED : ", errP)
			} else {
				strClientID := modules.ConvertSQLNullStringToString(rawClientID)
				strUsername := modules.ConvertSQLNullStringToString(rawUsername)
				strPassword := modules.ConvertSQLNullStringToString(rawPassword)
				strKey := modules.ConvertSQLNullStringToString(rawKey)
				strRemoteIP := modules.ConvertSQLNullStringToString(rawRemoteIP)

				redisKey := Config.ConstRedisAPIHitKey + strClientID

				mapRedis := make(map[string]interface{})
				mapRedis["username"] = strUsername
				mapRedis["password"] = strPassword
				mapRedis["key"] = strKey
				mapRedis["remoteip"] = strRemoteIP

				jsonRedisVal := modules.ConvertMapInterfaceToJSON(mapRedis)

				errR := modules.RedisSet(rc, cx, redisKey, jsonRedisVal, 0)
				if errR == nil {

				} else {
					fmt.Println("FAILED LOAD TO REDIS")
				}
			}
		}
	}

}


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
			loadCredentialToRedis(db, rc, cx)
			time.Sleep(10 * time.Second)
		}
	}()


	// APITransaction API
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Accept", "application/json")
		w.Header().Set("Content-type", "application/json")

		var bodyBytes []byte
		//var tracecodeX string
		var responseContent string
		var responseHeader = make(map[string]string)

		incTraceCode := modules.GenerateUUID()
		// Generate traceCode
		modules.DoLog("INFO", incTraceCode, "MAIN", "API",
			"Assigning TraceCode: "+incTraceCode, false, nil)

		incURL := fmt.Sprintf("%s", r.URL)[1:]

		if r.Body != nil && r.Method == "POST" {
			bodyBytes, _ = ioutil.ReadAll(r.Body)

			//fmt.Println(bodyBytes)
			var incomingBody string
			incomingBody = string(bodyBytes)
			incomingBody = strings.Replace(incomingBody, "\t", "", -1)
			incomingBody = strings.Replace(incomingBody, "\n", "", -1)
			incomingBody = strings.Replace(incomingBody, "\r", "", -1)

			// Write back the buffer to Body context, so it can be used by later process
			r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			modules.DoLog("INFO", incTraceCode, "MAIN", "API",
				"Incoming request: "+incomingBody, false, nil)

			remoteIPAddress := modules.GetIPAddress(r.RemoteAddr)

			var incomingHeader = make(map[string]interface{})
			incomingHeader["Content-Type"] = r.Header.Get("Content-Type")

			//modules.SaveIncomingRequest(db, incTraceCode, strURL, remoteIPAddress, incomingBody)

			mapIncoming := modules.ConvertJSONStringToMap("", incomingBody)
			//incReqType := modules.GetStringFromMapInterface(mapIncoming, "reqtype")

			// Route the request
			if incURL == "send" {
				_, responseHeader, responseContent = APISendData.Process(db, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
			}

			//modules.SaveIncomingResponse(db, tracecodeX, responseHeader, responseContent)

			modules.DoLog("INFO", incTraceCode, "MAIN", "API",
				"responseHeader: "+fmt.Sprintf("%+v", responseHeader)+", responseContent: "+responseContent,
				false, nil)
		}

		w.Write([]byte(responseContent))
	})

	log.Println("Starting HTTP Data Receiver API at port : " + Config.ConstAPIReceiverPort)
	http.ListenAndServe(":"+Config.ConstAPIReceiverPort, nil)

}