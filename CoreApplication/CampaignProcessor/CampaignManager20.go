package main

import (
	"billing/Config"
	"billing/CoreApplication/CampaignProcessor/ProcessorExcel"
	"billing/modules"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"runtime"
	"sync"
	"time"
)

var db *sql.DB

// RabbitMQ Connection
var conn *amqp.Connection

func doProcess() {
	var wg sync.WaitGroup

	// ExecuteBatchSMSSingleProcessor
	wg.Add(1)
	go ProcessorExcel.RunProcessorExcel(&wg)

	wg.Wait()
}

func main() {
	fmt.Println("Application Campaign Manager 2.0 is running ...")

	// use all 4 cores in server
	runtime.GOMAXPROCS(4)

	// Load configuration file
	modules.InitiateGlobalVariables(Config.ConstProduction)

	// Connect to DB
	var errDB error
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", modules.MapConfig["databaseHost"],
		modules.MapConfig["databasePort"], modules.MapConfig["databaseUser"], modules.MapConfig["databasePass"], modules.MapConfig["databaseName"])
	db, errDB = sql.Open("postgres", psqlInfo) // db udah di defined diatas, jadi harus pake = bukan :=

	if errDB != nil {
		modules.DoLog("INFO", "", "BatchMessageMerger_", "main",
			"Failed to connect to database server. Error!", true, errDB)
		panic(errDB)
	}

	db.SetConnMaxLifetime(time.Minute * 10)
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(10)

	defer db.Close()

	errDB = db.Ping()
	if errDB != nil {
		panic(errDB)
	}

	// Initiate RabbitMQ
	var errRabbit error
	conn, errRabbit = amqp.Dial("amqp://" + modules.MapConfig["rabbitUser"] + ":" + modules.MapConfig["rabbitPass"] + "@" + modules.MapConfig["rabbitHost"] + ":" + modules.MapConfig["rabbitPort"] + "/" + modules.MapConfig["rabbitVHost"])
	if errRabbit != nil {
		modules.DoLog("INFO", "", "MultiRouter", "main",
			"Failed to connect to RabbitMQ server. Error", true, errRabbit)

		panic(errRabbit)
	}
	defer conn.Close()

	doProcess()
}
