package ProcessorExcel

import (
	"billing/modules"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
	"strings"
	"sync"
	"time"
)

// DB Connection
var dbPostgres *sql.DB
var dbMongo *mongo.Database
var rc *redis.Client
var cx context.Context

// RabbitMQ Connection
var conn *amqp.Connection
var chIncoming *amqp.Channel

var incomingPrefetchCount = 1

var incomingQueueName = "EXCEL_INCOMING_DATA"

// var fileDirectory = "/app/web_wadyn_client/public/storage/app/public/autogenbnumber/"
var fileDirectory = "/3c/app/SETTLEMENT/upload_excel/"

func doProcessTheBatch(queueMessage string) {
	// Convert queueMessage to JSON MAP
	jsonQueueMessage := modules.ConvertJSONStringToMap("", queueMessage)

	// Extract the parameter
	if len(jsonQueueMessage) > 0 {
		batchId := modules.GetStringFromMapInterface(jsonQueueMessage, "batchId")

		// Get detail batchId from database
		query := `select client_id, formula_name, formulas, path_file_name, schedule_type, schedule from excel_upload where batch_id = $1`

		rows, err := dbPostgres.Query(query, batchId)

		if err != nil {
			modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
				"Failed to read table client group. Error occured.", true, err)
		} else {
			for rows.Next() {
				//var batchId string
				var xClientId sql.NullString
				var xFormulaName sql.NullString
				var xFormulas sql.NullString
				var xPathFileName sql.NullString
				var xScheduleType sql.NullString
				var xSchedule sql.NullTime

				errScan := rows.Scan(&xClientId, &xFormulaName, &xFormulas, &xPathFileName, &xScheduleType, &xSchedule)

				if errScan != nil {
					modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
						"Failed to get scan table batchtogo. Error occured.", true, errScan)
				} else {
					modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
						"Success to get data batchtogo. Processing", false, nil)

					traceCode := modules.GenerateUUID()
					// Generate traceCode
					modules.DoLog("INFO", traceCode, "MAIN", "API",
						"Assigning TraceCode: "+traceCode, false, nil)

					clientId := modules.ConvertSQLNullStringToString(xClientId)
					formulaName := modules.ConvertSQLNullStringToString(xFormulaName)
					formulas := modules.ConvertSQLNullStringToString(xFormulas)
					pathFileName := modules.ConvertSQLNullStringToString(xPathFileName)
					scheduleType := modules.ConvertSQLNullStringToString(xScheduleType)
					schedule := modules.ConvertSQLNullTimeToTime(xSchedule)

					// Read the Excel file
					fileFullPath := fileDirectory + pathFileName

					f, errX := excelize.OpenFile(fileFullPath)

					if errX != nil {
						modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
							"Failed to find excel file "+fileFullPath+". Error occured.", true, err)
					} else {
						modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
							"Success opening file "+fileFullPath+".", false, nil)

						// Get first sheet name
						sheetName := f.GetSheetName(1)
						modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
							"Sheet name: "+sheetName, false, nil)

						// Read row
						rowsXLSX := f.GetRows(sheetName)
						msisdnColNo := 0

						var fields []string
						var mapFormulaAtt = make(map[string]interface{})
						// Get other fields after MSISDN /* GOTTA BE DELETE SOON */
						for colNo := 0; colNo < len(rowsXLSX[0]); colNo++ {
							//if strings.ToUpper(rowsXLSX[0][colNo]) == "FORMULA" {
							//	msisdnColNo = colNo
							//	formula = rowsXLSX[1][colNo]
							//} else {
							//	fields = append(fields, rowsXLSX[0][colNo])
							//}
							fields = append(fields, rowsXLSX[0][colNo])
						}
						mapFormulaAtt["clientid"] = clientId
						mapFormulaAtt["name"] = formulaName
						mapFormulaAtt["formula"] = formulas
						mapFormulaAtt["type"] = scheduleType
						mapFormulaAtt["time"] = schedule
						isSuccess := false
						strFormulaID := ""
						if json.Valid([]byte(formulas)) {
							/* SAVE FORMULA ONLY RETURN WITH FORMULA ID THEN MERGE WITH DATA VALUES */
							isSuccess, strFormulaID, _ = modules.SaveFormulaArrayBillingIntoPg(dbPostgres, traceCode, mapFormulaAtt)
						} else {
							/* SAVE FORMULA ONLY RETURN WITH FORMULA ID THEN MERGE WITH DATA VALUES */
							isSuccess, strFormulaID = modules.SaveFormulaBillingIntoPg(dbPostgres, traceCode, mapFormulaAtt)
						}

						if isSuccess {
							modules.ReloadFormulaToRedis(dbPostgres, rc, cx, strFormulaID)
						}
						var mapIncomingExcel map[string]interface{}
						mapIncomingExcel["formulaid"] = strFormulaID
						mapIncomingExcel["clientid"] = clientId

						var mapRows []map[string]interface{}

						// Loop by MSISDN registered in first column
						for rowNo := 1; rowNo < len(rowsXLSX); rowNo++ {
							//processedMessage := theMessage

							// Get phoneNumber
							theMSISDN := strings.TrimSpace(rowsXLSX[rowNo][msisdnColNo])
							modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
								fmt.Sprintf("rowNo: %d -> theMSISDN: %s", rowNo, theMSISDN), false, nil)

							var mapRow = make(map[string]interface{})

							// Get other fields after MSISDN
							for colNo := 0; colNo < len(rowsXLSX[rowNo]); colNo++ {
								if msisdnColNo != colNo {
									theField := rowsXLSX[0][colNo]

									fmt.Printf("Processing row: %d - col: %d - theField: %s\n", rowNo, colNo, theField)

									modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
										fmt.Sprintf("rowNo: %d, colNo: %d -> theMSISDN: %s", rowNo, colNo, theMSISDN), false, nil)

									// Update message
									//processedMessage = strings.ReplaceAll(processedMessage, "["+theField+"]", rowsXLSX[rowNo][colNo])
									//fmt.Printf("-> theMessage: %s\n", processedMessage)

									/* ROW MAPPING */
									mapRow[theField] = rowsXLSX[rowNo][colNo]
								} else {
									modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
										fmt.Sprintf("Skip MSISDN Column rowNo: %d, colNo: %d -> theMSISDN: %s", rowNo, colNo, theMSISDN), false, nil)
								}
							}

							mapRows = append(mapRows, mapRow)

							//modules.DoLog("INFO", "", "ProcessorExcel", "doProcessTheBatch",
							//	"MSISDN: "+theMSISDN+" -> the message: "+processedMessage, false, nil)
							//destinationType := "UPLOAD"
							//go processTheData(batchId, clientId, apiUserName, smsType, clientSenderId, destinationType, theMSISDN, encoding, time.Now(), processedMessage, remoteIPAddress, defaultMessageType)
						}
						mapIncomingExcel["datas"] = mapRows

						/* SAVING DATA IN THIS LINE AFTER LOOP */
						modules.SaveDataBillingIntoMongo(dbMongo, rc, cx, traceCode, mapIncomingExcel)
					}
				}
			}
		}
	}

}

func doRunReadingQueue() {

	// Declaring queue if not exist
	queueIncoming, errIncoming := chIncoming.QueueDeclare(
		incomingQueueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if errIncoming != nil {
		modules.DoLog("INFO", "", "ProcessorExcel", "readingQueue",
			"Failed to connect to queue "+incomingQueueName+". Error occured.", true, errIncoming)

		panic(errIncoming)
	}

	_ = chIncoming.Qos(incomingPrefetchCount, 0, false)

	// Consume the queue
	message, err := chIncoming.Consume(
		queueIncoming.Name, // queue
		"",                 // consumer
		false,              // auto-ack false, need to manually ack!
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)

	// Thread utk check status chIncoming and reconnect if failed. Do it in different treads run forever
	go func() {
		for {
			fmt.Println("Checking chIncoming " + queueIncoming.Name + " state if is opened or not.")

			theCheckedQueue, errC := chIncoming.QueueInspect(queueIncoming.Name)

			if errC != nil {
				modules.DoLog("INFO", "", "ProcessorExcel", "readingQueue",
					"Checking queue status "+incomingQueueName+" is failed for error. DO RE-INITIATE INCOMING CHANNEL.", true, errC)

				// Re-initiate chIncoming
				chIncoming, _ = conn.Channel()
				_ = chIncoming.Qos(incomingPrefetchCount, 0, false)
			} else {
				if theCheckedQueue.Consumers == 0 {
					modules.DoLog("INFO", "", "ProcessorExcel", "readingQueue",
						"Consumer of the incoming queue: "+incomingQueueName+" is 0. DO RE-INITIATE RABBITMQ CHANNEL.", false, nil)

					// Re-initiate chIncoming
					chIncoming, _ = conn.Channel()
					_ = chIncoming.Qos(incomingPrefetchCount, 0, false)
				}
			}

			sleepDuration := 1 * time.Minute
			time.Sleep(sleepDuration)
		}
	}()

	if err != nil {
		modules.DoLog("INFO", "", "ProcessorExcel", "readingQueue",
			"Failed to consume queue "+incomingQueueName+". Error occured.", true, err)
	} else {
		forever := make(chan bool)

		for d := range message {
			fmt.Printf("Receive message: %s \n", d.Body)
			modules.DoLog("INFO", "", "ProcessorExcel", "readingQueue",
				"Receiving message: "+string(d.Body), false, nil)

			// do the process with rateLimit transaction per second
			modules.DoLog("INFO", "", "ProcessorExcel", "readingQueue",
				"Do processing the incoming message from queue "+incomingQueueName, false, nil)

			go doProcessTheBatch(string(d.Body)) // Jangan dipanggil sebagai GO ROUTINE, potensi data racing and error. Speed is high enough without calling as go routine

			fmt.Println("Done processing queue.")
			modules.DoLog("DEBUG", "", "ProcessorExcel", "readingQueue",
				"Done Processing queue message. Sending ack to rabbitmq.", false, nil)

			errx := d.Ack(false)

			if errx != nil {
				modules.DoLog("DEBUG", "", "ProcessorExcel", "readingQueue",
					"Failed to acknowledge manually message: "+string(d.Body)+". STOP the transceiver.", false, nil)

				os.Exit(-1)
			}
		}

		fmt.Println("[*] Waiting for messages. To exit press CTRL-C")
		<-forever
	}
}

func RunProcessorExcel(wg *sync.WaitGroup) {
	defer wg.Done()

	modules.DoLog("INFO", "", "RunWADYNSMSPersonalizedProcessor", "main",
		"WADYN SMS Personalized Processor starts.", false, nil)

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

	// Initiate RabbitMQ
	var errRabbit error
	conn, errRabbit = amqp.Dial("amqp://" + modules.MapConfig["rabbitUser"] + ":" + modules.MapConfig["rabbitPass"] + "@" + modules.MapConfig["rabbitHost"] + ":" + modules.MapConfig["rabbitPort"] + "/" + modules.MapConfig["rabbitVHost"])
	if errRabbit != nil {
		modules.DoLog("INFO", "", "RunWADYNSMSPersonalizedProcessor", "main",
			"Failed to connect to RabbitMQ server. Error", true, errRabbit)

		panic(errRabbit)
	}

	// Initiate RabbitMQ Channel Publisher
	//var errP error
	//chPublisher, errP = conn.Channel()
	//if errP != nil {
	//	modules.DoLog("INFO", "", "RunWADYNSMSPersonalizedProcessor", "main",
	//		"Failed to create channel to rabbitmq to publish to router.", true, errP)
	//
	//	panic(errP)
	//} // No QOS

	// Declare rabbitMQ Channel
	var errI error
	chIncoming, errI = conn.Channel()
	if errI != nil {
		modules.DoLog("INFO", "", "RunWADYNSMSPersonalizedProcessor", "main",
			"Failed to create channel to rabbitmq.", true, errI)
	}

	defer conn.Close()

	doRunReadingQueue()
}
