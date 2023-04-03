package main

import (
	"billing/Config"
	"billing/CoreApplication/APIClient/APICredential"
	"billing/CoreApplication/APIClient/APIFormula/APIActivateFormula"
	"billing/CoreApplication/APIClient/APIFormula/APIGetFormula"
	"billing/CoreApplication/APIClient/APIFormula/APINewFormula"
	"billing/CoreApplication/APIClient/APIFormula/APIUpdateFormula"
	"billing/CoreApplication/APIClient/APIGetData"
	"billing/modules"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"
)

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

	// Initiate Redis
	rc = modules.InitiateRedisClient()
	cx = context.Background()
	errRedis := rc.Ping(cx).Err()
	if errRedis != nil {
		panic(errRedis)
	} else {
		fmt.Println("Success connected to Redis")
	}

	/* Setup MongoDB */
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

			//modules.SaveIncomingRequest(dbPostgres, incTraceCode, strURL, remoteIPAddress, incomingBody)

			mapIncoming := modules.ConvertJSONStringToMap("", incomingBody)
			incReqType := modules.GetStringFromMapInterface(mapIncoming, "reqtype")

			// Route the request
			if incURL == "formula" {
				if strings.ToUpper(incReqType) == "NEW" {
					_, responseHeader, responseContent = APINewFormula.Process(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "UPDATE" {
					_, responseHeader, responseContent = APIUpdateFormula.Process(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "ACTIVE" || strings.ToUpper(incReqType) == "DEACTIVE" {
					_, responseHeader, responseContent = APIActivateFormula.Process(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "GET_ALL" {
					_, responseHeader, responseContent = APIGetFormula.ProcessGetAll(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "GET_BY_ID" {
					_, responseHeader, responseContent = APIGetFormula.ProcessGetById(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				}
			} else if incURL == "data-formula" {
				if strings.ToUpper(incReqType) == "GET_ALL" {
					_, responseHeader, responseContent = APIGetData.ProcessGetAll(dbPostgres, dbMongo, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "GET_BY_ID" {
					_, responseHeader, responseContent = APIGetData.ProcessGetById(dbPostgres, dbMongo, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				}
			} else if incURL == "token" {
				if strings.ToUpper(incReqType) == "GET_TOKEN" {
					_, responseHeader, responseContent = APICredential.ProcessGetNewToken(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "REFRESH_TOKEN" {
					_, responseHeader, responseContent = APICredential.ProcessRefreshToken(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				} else if strings.ToUpper(incReqType) == "DELETE_TOKEN" {
					_, responseHeader, responseContent = APICredential.ProcessDeleteToken(dbPostgres, rc, cx, incTraceCode, incomingHeader, mapIncoming, remoteIPAddress)
				}
			}

			//modules.SaveIncomingResponse(dbPostgres, tracecodeX, responseHeader, responseContent)

			modules.DoLog("INFO", incTraceCode, "MAIN", "API",
				"responseHeader: "+fmt.Sprintf("%+v", responseHeader)+", responseContent: "+responseContent,
				false, nil)
		}

		w.Write([]byte(responseContent))
	})

	log.Println("Starting HTTP API Admin at port : " + Config.ConstAPIClientPort)
	http.ListenAndServe(":"+Config.ConstAPIClientPort, nil)

}
