package modules

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mutex = &sync.Mutex{}

func TransactionMgmtSaveInitialTransactionToDBAndRedis(messageId string, db *sql.DB, receiverDateTime time.Time, batchId string,
	msisdn string, message string, countryCode string, prefix string, telecomId string, trxStatus string, smsChannel string, product string,
	clientSenderIdId string, clientSenderId string, clientId string, apiUserName string, clientUnitPrice float64, currency string, messageEncoding string,
	messageLength int32, smsCount int32, sysSessionId string, autogenPrefix string) bool {
	// product is SMS, WA, VOICE in capital
	clientTotalPrice := float64(smsCount) * clientUnitPrice
	isSuccess := false

	queryInsert := "INSERT INTO transaction_sms(message_id, transaction_date, msisdn, message, country_code, telecom_id, " +
		"prefix, status_code, receiver_type, application_id, client_id, currency, message_encodng, message_length, " +
		"sms_count, client_price_per_unit, client_price_total, client_sender_id, batch_id, api_username) VALUES " +
		"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)"

	result, err := db.Exec(queryInsert, messageId, receiverDateTime, msisdn, message, countryCode, telecomId, prefix,
		trxStatus, smsChannel, product, clientId, currency, messageEncoding, messageLength, smsCount, clientUnitPrice,
		clientTotalPrice, clientSenderId, batchId, apiUserName)

	if err != nil {
		// Failed insert
		isSuccess = false

		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveInitialTransaction",
			"Failed to save to database transaction. Error occured.", true, err)
	} else {
		rows, _ := result.RowsAffected()

		//if errX != nil {
		//	// Failed to insert DB
		//	isSuccess = false
		//
		//	DoLog("INFO", messageId,"TRANSACTIONMANAGEMENT", "SaveInitialTransaction",
		//		"Failed to save to database transaction - failed get row affected. Error occured.", true, errX)
		//} else {
		if rows != 1 {
			// Failed to insert DB
			isSuccess = false

			DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveInitialTransaction",
				"Failed to save to database transaction - affected row != 1.", false, nil)
		} else {
			isSuccess = true

			DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveInitialTransaction",
				"Success to save to database transaction.", false, nil)

			// Saving into redis
			//TransactionMgmtInsertTransactionInRedis(messageId, clientId, clientSenderIdId, clientSenderId,
			//	DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", receiverDateTime), smsChannel,
			//	apiUserName, msisdn, message, countryCode, prefix, telecomId, trxStatus, sysSessionId, autogenPrefix)
		}
		//}
	}

	return isSuccess
}

func TransactionMgmtSaveReceiverTransaction(messageId string, db *sql.DB, receiverDateTime time.Time, receiverData string,
	receiverClientResponse string, remoteIpAddress string, receiverClientResponseDateTime time.Time) {

	queryInsert := "INSERT INTO transaction_sms_receiver(message_id, receiver_date_time, receiver_data, " +
		"receiver_client_response, client_ip_address, receiver_client_response_date_time) VALUES ($1, $2, $3, $4, $5, $6)"

	_, err := db.Exec(queryInsert, messageId, receiverDateTime, receiverData, receiverClientResponse, remoteIpAddress, receiverClientResponseDateTime)

	if err != nil {
		// Failed insert
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveInitialReceiverTransaction",
			"Failed to save to database transaction receiver. Error occured.", true, err)
	} else {
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveInitialReceiverTransaction",
			"Success to save to database transaction receiver.", false, nil)
	}
}

func TransactionMgmtSaveTransactionDLR(messageId string, db *sql.DB, clientId string, dlrDateTime time.Time, dlrBody string,
	dlrStatus string, dlrPushTo string, dlrClientPushResponse string, dlrLogNote string) {
	queryInsert := "INSERT INTO transaction_sms_dlr(message_id, client_id, dlr_date_time, dlr_body, dlr_status, dlr_push_to, " +
		"dlr_client_push_response, dlr_log_note) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

	_, err := db.Exec(queryInsert, messageId, clientId, dlrDateTime, dlrBody, dlrStatus, dlrPushTo, dlrClientPushResponse, dlrLogNote)

	if err != nil {
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveTransactionDLR",
			"Failed to save to database transaction dlr. Error occured.", true, err)
	} else {
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveTransactionDLR",
			"Success to save to database transaction dlr.", false, nil)
	}
}

func TransactionMgmtSaveTransactionCallback(messageId string, db *sql.DB, callbackUrl string, callbackMethod string, clientId string) {
	query := "INSERT INTO transaction_sms_api_callback(message_id, callback_url, callback_method, client_id) VALUES ($1, $2, $3, $4)"

	_, err := db.Exec(query, messageId, callbackUrl, callbackMethod, clientId)

	if err != nil {
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveTransactionCallback",
			"Failed to save to database transaction callback. Error occured.", true, err)
	} else {
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "SaveTransactionCallback",
			"Success to save to database transaction callback.", false, nil)
	}
}

func TransactionMgmtGetCallbackUrlAndMethod(messageId string, db *sql.DB) (string, string, string) {
	callbackUrl := ""
	callbackMethod := ""
	clientId := ""

	query := "select callback_url, callback_method, client_id from transaction_sms_api_callback where message_id = $1"

	rows, err := db.Query(query, messageId)

	if err != nil {
		DoLog("INFO", messageId, "BlastMeTransactionManagement", "GetCallbackUrlAndMethod",
			"Failed to get callback url and method. Error occured.", true, err)
	} else {
		for rows.Next() {
			errScan := rows.Scan(&callbackUrl, &callbackMethod, &clientId)

			if errScan != nil {
				DoLog("INFO", messageId, "BlastMeTransactionManagement", "GetCallbackUrlAndMethod",
					"Failed to get data callback url and method. Error occured.", true, errScan)
			} else {
				DoLog("INFO", messageId, "BlastMeTransactionManagement", "GetCallbackUrlAndMethod",
					"Success to get callback url: "+callbackUrl+", callbackMethod: "+callbackMethod, false, nil)
			}
		}
	}

	return callbackUrl, callbackMethod, clientId
}

// noinspection GoUnusedExportedFunction
func TransactionMgmtGetTransactionDetail(messageId string, db *sql.DB) map[string]interface{} {
	queryTransaction := "select transaction_date, msisdn, message, country_code, telecom_id, prefix, status_code, receiver_type, " +
		"client_id, currency, message_encodng, message_length, sms_count, client_price_per_unit, client_price_total, " +
		"client_sender_id, batch_id, api_username from transaction_sms where message_id = '" + messageId + "'"

	var mapDetailTransaction = make(map[string]interface{})

	rows, err := db.Query(queryTransaction)

	fmt.Println("===> Query : ", queryTransaction)
	fmt.Println("===> Message ID : ", messageId)

	if err != nil {
		if err == sql.ErrNoRows {
			DoLog("INFO", "", "BlastMeFunctions", "GetTransactionDetail",
				"No row returned.", true, err)
		} else {
			DoLog("INFO", "", "BlastMeFunctions", "GetTransactionDetail",
				"Failed to read database transaction. Error occured!.", true, err)
		}

		fmt.Println("===> Position : Error 1 ")

		mapDetailTransaction = nil
	} else {
		for rows.Next() {
			var transactionDateTime time.Time
			var msisdn string
			var message string
			var countryCode string
			var telecomId string
			var prefix string
			var statusCode string
			var receiverType string
			var clientId string
			var currency string
			var messageEncoding string
			var messageLength int64
			var smsCount int64
			var clientPricePerUnit float64
			var clientPriceTotal float64
			var clientSenderiId string
			var batchId string
			var apiUserName string

			errScan := rows.Scan(&transactionDateTime, &msisdn, &message, &countryCode, &telecomId, &prefix, &statusCode, &receiverType,
				&clientId, &currency, &messageEncoding, &messageLength, &smsCount, &clientPricePerUnit, &clientPriceTotal, &clientSenderiId,
				&batchId, &apiUserName)

			if errScan != nil {
				DoLog("INFO", "", "BlastMeFunctions", "GetTransactionDetail",
					"Failed to read database transaction. Error occured.", true, err)

				fmt.Println("===> Position : Error 2 ")

			} else {
				fmt.Println("transactionDateTime: " + DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", transactionDateTime) + ", msisdn: " + msisdn +
					", message: " + message + ", countryCode: " + countryCode + ", telecomId: " + telecomId + ", prefix: " + prefix +
					", statusCode: " + statusCode + ", receiverType: " + receiverType + ", clientId: " + clientId + ", currency" + currency +
					", messageEncoding: " + messageEncoding + ", messageLength: " + strconv.FormatInt(messageLength, 10) + ", smsCount: " + strconv.FormatInt(smsCount, 10) +
					", clientPricePerUnit: " + fmt.Sprintf("%.2f", clientPricePerUnit) + ", clientPriceTotal: " + fmt.Sprintf("%.2f", clientPriceTotal) +
					", clientSenderId: " + clientSenderiId + ", batchId: " + batchId)

				mapDetailTransaction["transactionDateTime"] = transactionDateTime
				mapDetailTransaction["msisdn"] = msisdn
				mapDetailTransaction["message"] = message
				mapDetailTransaction["countryCode"] = countryCode
				mapDetailTransaction["telecomId"] = telecomId
				mapDetailTransaction["prefix"] = prefix
				mapDetailTransaction["statusCode"] = statusCode
				mapDetailTransaction["receiverType"] = receiverType
				mapDetailTransaction["clientId"] = clientId
				mapDetailTransaction["currency"] = currency
				mapDetailTransaction["messageEncoding"] = messageEncoding
				mapDetailTransaction["messageLength"] = messageLength
				mapDetailTransaction["smsCount"] = smsCount
				mapDetailTransaction["clientPricePerUnit"] = clientPricePerUnit
				mapDetailTransaction["clientPriceTotal"] = clientPriceTotal
				mapDetailTransaction["clientSenderId"] = clientSenderiId
				mapDetailTransaction["batchId"] = batchId
				mapDetailTransaction["apiUserName"] = apiUserName

				fmt.Println("===> Position : Success get data ")

			}
		}
	}
	defer rows.Close()

	fmt.Println("===> Return : ", mapDetailTransaction)

	return mapDetailTransaction
}

func TransactionMgmtGetTransactionDetailfromVendorMessageId(vendorMessageId string, db *sql.DB) (string, map[string]interface{}) {
	queryTransaction := "select transaction_date, trx.message_id, msisdn, message, country_code, telecom_id, prefix, status_code, receiver_type, " +
		"client_id, currency, message_encodng, message_length, sms_count, client_price_per_unit, client_price_total, " +
		"client_sender_id, batch_id, api_username from transaction_sms trx \nleft join transaction_sms_vendor vnd on trx.message_id = vnd.message_id " +
		"where vnd.vendor_message_id = '" + vendorMessageId + "'"

	var mapDetailTransaction = make(map[string]interface{})
	strMessageId := ""

	rows, err := db.Query(queryTransaction)

	fmt.Println("===> Query : ", queryTransaction)
	fmt.Println("===> Vendor Message ID : ", vendorMessageId)

	if err != nil {
		if err == sql.ErrNoRows {
			DoLog("INFO", "", "BlastMeFunctions", "GetTransactionDetail",
				"No row returned.", true, err)
		} else {
			DoLog("INFO", "", "BlastMeFunctions", "GetTransactionDetail",
				"Failed to read database transaction. Error occured!.", true, err)
		}

		fmt.Println("===> Position : Error 1 ")

		mapDetailTransaction = nil
	} else {
		for rows.Next() {
			var transactionDateTime time.Time
			var messageId string
			var msisdn string
			var message string
			var countryCode string
			var telecomId string
			var prefix string
			var statusCode string
			var receiverType string
			var clientId string
			var currency string
			var messageEncoding string
			var messageLength int64
			var smsCount int64
			var clientPricePerUnit float64
			var clientPriceTotal float64
			var clientSenderiId string
			var batchId string
			var apiUserName string

			errScan := rows.Scan(&transactionDateTime, &messageId, &msisdn, &message, &countryCode, &telecomId, &prefix, &statusCode, &receiverType,
				&clientId, &currency, &messageEncoding, &messageLength, &smsCount, &clientPricePerUnit, &clientPriceTotal, &clientSenderiId,
				&batchId, &apiUserName)

			if errScan != nil {
				DoLog("INFO", "", "BlastMeFunctions", "GetTransactionDetail",
					"Failed to read database transaction. Error occured.", true, err)

				fmt.Println("===> Position : Error 2 ")

			} else {
				fmt.Println("transactionDateTime: " + DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", transactionDateTime) + ", msisdn: " + msisdn +
					", messageId: " + messageId + ", message: " + message + ", countryCode: " + countryCode + ", telecomId: " + telecomId + ", prefix: " + prefix +
					", statusCode: " + statusCode + ", receiverType: " + receiverType + ", clientId: " + clientId + ", currency" + currency +
					", messageEncoding: " + messageEncoding + ", messageLength: " + strconv.FormatInt(messageLength, 10) + ", smsCount: " + strconv.FormatInt(smsCount, 10) +
					", clientPricePerUnit: " + fmt.Sprintf("%.2f", clientPricePerUnit) + ", clientPriceTotal: " + fmt.Sprintf("%.2f", clientPriceTotal) +
					", clientSenderId: " + clientSenderiId + ", batchId: " + batchId)

				mapDetailTransaction["transactionDateTime"] = transactionDateTime
				mapDetailTransaction["msisdn"] = msisdn
				mapDetailTransaction["message"] = message
				mapDetailTransaction["countryCode"] = countryCode
				mapDetailTransaction["telecomId"] = telecomId
				mapDetailTransaction["prefix"] = prefix
				mapDetailTransaction["statusCode"] = statusCode
				mapDetailTransaction["receiverType"] = receiverType
				mapDetailTransaction["clientId"] = clientId
				mapDetailTransaction["currency"] = currency
				mapDetailTransaction["messageEncoding"] = messageEncoding
				mapDetailTransaction["messageLength"] = messageLength
				mapDetailTransaction["smsCount"] = smsCount
				mapDetailTransaction["clientPricePerUnit"] = clientPricePerUnit
				mapDetailTransaction["clientPriceTotal"] = clientPriceTotal
				mapDetailTransaction["clientSenderId"] = clientSenderiId
				mapDetailTransaction["batchId"] = batchId
				mapDetailTransaction["apiUserName"] = apiUserName

				strMessageId = messageId

				fmt.Println("===> Position : Success get data ")

			}
		}
	}
	defer rows.Close()

	fmt.Println("===> Return : ", mapDetailTransaction)

	return strMessageId, mapDetailTransaction
}

// noinspection GoUnusedExportedFunction
func TransactionMgmtUpdateTransactionStatus(messageId string, db *sql.DB, newTransactionStatus string) bool {
	isUpdateSuccess := false

	queryUpdate := "update transaction_sms set status_code = $1 where message_id = $2"
	fmt.Println(queryUpdate + ", newTrxStatus: " + newTransactionStatus + ", messageId: " + messageId)

	DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
		"Start updating.", false, nil)
	//updateResult, err := db.Exec(queryUpdate, newTransactionStatus, messageId)
	_, err := db.Exec(queryUpdate, newTransactionStatus, messageId)
	DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
		"Done updating.", false, nil)

	if err != nil {
		isUpdateSuccess = false

		// Error update
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
			"Failed to update database transaction. Error occured.", true, err)
	} else {
		queryUpdateTrxStatus := "update transaction_sms_status set final_status = $1 where message_id = $2"

		fmt.Println(queryUpdate + ", newTrxStatus in transaction SMS Status: " + newTransactionStatus + ", messageId: " + messageId)

		DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
			"Start updating new transaction SMS status.", false, nil)
		//updateResult, err := db.Exec(queryUpdate, newTransactionStatus, messageId)
		_, _ = db.Exec(queryUpdateTrxStatus, newTransactionStatus, messageId)
		DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
			"Done updating transaction SMS status.", false, nil)

		isUpdateSuccess = true
	}

	return isUpdateSuccess
}

func TransactionMgmtUpdateTransactionStatusVoice(messageId string, db *sql.DB, newTransactionStatus string, callDuration int64, totalVoicePrice float64) bool {
	isUpdateSuccess := false

	queryUpdate := "update transaction_sms set status_code = $1, call_duration = $2, client_price_total = client_price_total + $3 where message_id = $4"
	fmt.Println(queryUpdate + ", newTrxStatus: " + newTransactionStatus + ", messageId: " + messageId)

	DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
		"Start updating.", false, nil)
	//updateResult, err := db.Exec(queryUpdate, newTransactionStatus, messageId)
	_, err := db.Exec(queryUpdate, newTransactionStatus, callDuration, totalVoicePrice, messageId)
	DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
		"Done updating.", false, nil)

	if err != nil {
		isUpdateSuccess = false

		// Error update
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
			"Failed to update database transaction. Error occured.", true, err)
	} else {
		queryUpdateTrxStatus := "update transaction_sms_status set final_status = $1 where message_id = $2"

		fmt.Println(queryUpdate + ", newTrxStatus in transaction SMS Status: " + newTransactionStatus + ", messageId: " + messageId)

		DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
			"Start updating new transaction SMS status.", false, nil)
		//updateResult, err := db.Exec(queryUpdate, newTransactionStatus, messageId)
		_, _ = db.Exec(queryUpdateTrxStatus, newTransactionStatus, messageId)
		DoLog("DEBUG", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionStatus",
			"Done updating transaction SMS status.", false, nil)

		isUpdateSuccess = true
	}

	return isUpdateSuccess
}

func TransactionMgmtUpdateTransactionStatusAndVoiceDuration(messageId string, db *sql.DB, newTransactionStatus string, callDuration int, clientId string) bool {
	isUpdateSuccess := false

	queryUpdate := "update transaction_sms set status_code = $1, call_duration = $2, client_price_total = client_price_total + $3 where message_id = $4"
	fmt.Println(queryUpdate + ", newTrxStatus: " + newTransactionStatus + ", messageId: " + messageId)

	updateResult, err := db.Exec(queryUpdate, newTransactionStatus, callDuration, callDuration, messageId)

	if err != nil {
		isUpdateSuccess = false

		// Error update
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "TransactionMgmtUpdateTransactionStatusAndVoiceDuration",
			"Failed to update database transaction. Error occured.", true, err)
	} else {
		// Check row impacted
		numImpacted, _ := updateResult.RowsAffected()

		if numImpacted > 0 {
			isUpdateSuccess = true

			// Deduct balance again
			//TransactionMgmtDeductBalance(messageId, db, clientId, )

			// Success update
			DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "TransactionMgmtUpdateTransactionStatusAndVoiceDuration",
				"Success to update database transaction.", false, nil)
		} else {
			isUpdateSuccess = false

			// Success update
			DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "TransactionMgmtUpdateTransactionStatusAndVoiceDuration",
				"Failed to update database transaction. Parameter messageId: "+messageId+" data not found.", false, nil)
		}
	}

	return isUpdateSuccess
}

// noinspection GoUnusedExportedFunction
func TransactionMgmtUpdateTransactionVendorStatus(messageId string, db *sql.DB, callbackDateTime time.Time, vendorCallback string, vendorTrxStatus string) {
	queryUpdate := "update transaction_sms_vendor set vendor_callback_date_time = $1, vendor_callback = $2, vendor_trx_status = $3 where message_id = $4"

	_, err := db.Exec(queryUpdate, callbackDateTime, vendorCallback, vendorTrxStatus, messageId)

	if err != nil {
		// Error update
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionVendorStatus",
			"Failed to save to database transaction. Error occured.", true, err)
	} else {
		// Success update
		DoLog("INFO", messageId, "TRANSACTIONMANAGEMENT", "UpdateTransactionVendorStatus",
			"Success to save to database transaction.", false, nil)
	}
}

// noinspection GoUnusedExportedFunction
func TransactionMgmtInsertIntoTransactionVendor(messageId string, db *sql.DB, vendorId string, vendorHitDateTime time.Time, vendorHitRequest string,
	vendorHitRespDateTime time.Time, vendorHitResponse string, vendorMessageId string, vendorTrxStatus string) {
	queryInsert := "INSERT INTO transaction_sms_vendor(message_id, vendor_id, vendor_hit_date_time, vendor_hit_request, " +
		"vendor_hit_resp_date_time, vendor_hit_response, vendor_message_id, router_to_transaceiver_date_time, vendor_trx_status) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err := db.Exec(queryInsert, messageId, vendorId, vendorHitDateTime, vendorHitRequest, vendorHitRespDateTime, vendorHitResponse, vendorMessageId, time.Now(), vendorTrxStatus)

	if err != nil {
		DoLog("INFO", "", "BlastMeTransactionManagement", "InsertIntoTransactionVendor",
			"Failed to save to table transaction vendor. Error occured.", true, err)
	} else {
		DoLog("INFO", "", "BlastMeTransactionManagement", "InsertIntoTransactionVendor",
			"Success to save to table transaction vendor.", false, nil)
	}
}

func GetMessageIdByVendorMessageId(db *sql.DB, vendorId string, vendorMessageId string) string {
	messageId := ""

	query := "select message_id from transaction_sms_vendor where vendor_message_id = $1 and vendor_id = $2"

	rows, err := db.Query(query, vendorMessageId, vendorId)

	if err != nil {
		DoLog("INFO", messageId, "BlastMeTransactionManagement", "GetMessageIdByVendorMessageId",
			"Failed to read vendor transaction. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var dataMessageId sql.NullString

			errScan := rows.Scan(&dataMessageId)

			if errScan != nil {
				// Failed to scan
				DoLog("INFO", "", "BlastMeTransactionManagement", "GetMessageIdByVendorMessageId",
					"Failed to scan transaction vendor. Error occured.", true, errScan)
			} else {
				messageId = ConvertSQLNullStringToString(dataMessageId)
			}
		}
	}

	return messageId
}

func GetDataIdByKeys(db *sql.DB, dataId string, formulaId string, clientId string) string {
	messageId := ""

	query := "select data_id from ytransaction_v2 where data_id = $1 and formula_id = $2 and client_id = $3"

	rows, err := db.Query(query, dataId, formulaId, clientId)

	if err != nil {
		DoLog("INFO", messageId, "BlastMeTransactionManagement", "GetMessageIdByVendorMessageId",
			"Failed to read result transaction. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var dataMessageId sql.NullString

			errScan := rows.Scan(&dataMessageId)

			if errScan != nil {
				// Failed to scan
				DoLog("INFO", "", "BlastMeTransactionManagement", "GetMessageIdByVendorMessageId",
					"Failed to scan transaction result. Error occured.", true, errScan)
			} else {
				messageId = ConvertSQLNullStringToString(dataMessageId)
			}
		}
	}

	return messageId
}

// noinspection GoUnusedExportedFunction
func TransactionMgmtDeductBalance(messageId string, db *sql.DB, clientId string, deductionValue float64, lastUsageDateTime time.Time, lastUsageType string, lastUsageBy string) (bool, float64, float64, float64) {
	isSuccess := false
	prevBalance := 0.00
	usage := 0.00
	afterBalance := 0.00

	fmt.Println("Deduct - messageId: " + messageId + ", clientId: " + clientId + ", deductionValue: " + fmt.Sprintf("%.5f", deductionValue))

	//	query := "update client_balance set now_balance = now_balance - $1, last_usage_value = -1 * $2, last_usage_date_time = $3, last_usage_type = $4, last_usage_by = $5 where client_id = $6 returning now_balance, now_balance - last_usage_value as prev_balance, last_usage_value"
	query := "update client_balance set now_balance = now_balance - " + fmt.Sprintf("%.5f", deductionValue) + ", last_usage_value = -1 * " + fmt.Sprintf("%.5f", deductionValue) +
		", last_usage_date_time = $1, last_usage_type = $2, last_usage_by = $3 where client_id = $4 returning now_balance, now_balance - last_usage_value as prev_balance, last_usage_value"
	fmt.Println("query: " + query)

	//	rows, err := db.Query(query, deductionValue, deductionValue, lastUsageDateTime, lastUsageType, lastUsageBy, clientId)
	rows, err := db.Query(query, lastUsageDateTime, lastUsageType, lastUsageBy, clientId)

	if err != nil {
		isSuccess = false

		//panic(err)
		DoLog("INFO", messageId, "BlastMeTransactionManagement", "DeductBalance",
			"Failed to deduct balance clientId: "+clientId+". Error occured.", true, err)
	} else {
		for rows.Next() {
			errScan := rows.Scan(&afterBalance, &prevBalance, &usage)

			if errScan != nil {
				isSuccess = false

				DoLog("INFO", messageId, "BlastMeTransactionManagement", "DeductBalance",
					"Failed to deduct balance (scan) clientId: "+clientId+". Error occured.", true, errScan)
			} else {
				isSuccess = true

				DoLog("INFO", messageId, "BlastMeTransactionManagement", "DeductBalance",
					"Success to deduct balance clientId: "+clientId+". beforeBalance: "+fmt.Sprintf("%.5f", prevBalance)+
						", afterBalance: "+fmt.Sprintf("%.5f", afterBalance)+", usage: "+fmt.Sprintf("%.5f", usage), false, nil)
			}
		}
	}

	return isSuccess, prevBalance, usage, afterBalance
}

func TransactionMgmtGetClientBalance(messageId string, db *sql.DB, clientId string) float64 {
	balance := 0.00

	query := "select now_balance from client_balance where client_id = $1"

	rows, err := db.Query(query, clientId)

	if err != nil {
		DoLog("INFO", messageId, "BlastMeTransactionManagement", "TransactionMgmtGetClientBalance",
			"Failed to read client balance. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var nowBalance = 0.00

			errScan := rows.Scan(&nowBalance)

			if errScan != nil {
				// Failed to scan
				DoLog("INFO", "", "BlastMeTransactionManagement", "TransactionMgmtGetClientBalance",
					"Failed to scan client balance. Error occured.", true, errScan)
			} else {
				// Check balance success
				balance = nowBalance
			}
		}
	}

	return balance
}

func TransactionMgmtInsertTransactionInRedis(messageId string, clientId string, clientSenderIdId string,
	clientSenderId string, transactionDateTime string, smsChannel string, apiUserName string, msisdn string, message string,
	countryCode string, prefix string, telecomId string, errorCode string, sysSessionId string, autogenPrefix string) {
	var mapDataRedis = make(map[string]interface{})

	mapDataRedis["messageId"] = messageId
	mapDataRedis["clientId"] = clientId
	mapDataRedis["clientSenderIdId"] = clientSenderIdId
	mapDataRedis["clientSenderId"] = clientSenderId
	mapDataRedis["transactionDateTime"] = transactionDateTime
	mapDataRedis["smsChannel"] = smsChannel
	mapDataRedis["apiUserName"] = apiUserName
	mapDataRedis["msisdn"] = msisdn
	mapDataRedis["message"] = message
	mapDataRedis["countryCode"] = countryCode
	mapDataRedis["prefix"] = prefix
	mapDataRedis["telecomId"] = telecomId
	mapDataRedis["errorCode"] = errorCode
	mapDataRedis["sysSessionId"] = sysSessionId
	mapDataRedis["autogenPrefix"] = autogenPrefix

	// Convert to JSON
	jsonRedis := ConvertMapInterfaceToJSON(mapDataRedis)

	// Put into redis
	redisKey := "transactionData-" + messageId
	redisVal := jsonRedis

	RedisSetDataRedis(messageId, redisKey, redisVal)
}

func TransactionMgmtInsertTransactionFinancial(messageId string, db *sql.DB, usageType string, usageBy string, clientId string,
	transactionDateTime time.Time, usageDesc string, previousBalance float64, usageValue float64, afterBalance float64,
	vendorId string, vendorPrice float64, vendorCurrency string) bool {
	isSuccess := false

	queryInsert := "INSERT INTO transaction_sms_financial(message_id, usage_type, usage_by, client_id, transaction_datetime, " +
		"description, previous_balance, usage, after_balance, vendor_id, vendor_price, vendor_currency) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"

	resultInsert, err := db.Exec(queryInsert, messageId, usageType, usageBy, clientId, transactionDateTime, usageDesc, previousBalance,
		usageValue, afterBalance, vendorId, vendorPrice, vendorCurrency)

	if err != nil {
		isSuccess = false

		DoLog("INFO", messageId, "BlastMeTransactionManagement", "TransactionMgmtInsertTransactionFinancial",
			"Failed to insert transaction financial data. Error occured.", true, err)
	} else {
		rows, errX := resultInsert.RowsAffected()

		if errX != nil {
			isSuccess = false

			DoLog("INFO", messageId, "BlastMeTransactionManagement", "TransactionMgmtInsertTransactionFinancial",
				"Failed to insert transaction financial data. Failed to get row affected.", true, errX)
		} else {
			if rows != 1 {
				isSuccess = false

				DoLog("INFO", messageId, "BlastMeTransactionManagement", "TransactionMgmtInsertTransactionFinancial",
					"Failed to insert transaction financial data. Row affected is 0.", false, nil)
			} else {
				isSuccess = true

				DoLog("INFO", messageId, "BlastMeTransactionManagement", "TransactionMgmtInsertTransactionFinancial",
					"Success to insert transaction financial data.", false, nil)
			}
		}
	}

	return isSuccess
}

func SaveDataBillingIntoMongo(db *mongo.Database, rc *redis.Client, cx context.Context, incTraceCode string, mapIncoming map[string]interface{}) bool {

	var incDatas []map[string]interface{}
	//isSuccess := true

	incClientID := GetStringFromMapInterface(mapIncoming, "clientid")
	incFormulaID := GetStringFromMapInterface(mapIncoming, "formulaid")
	collectionName := incClientID + "_" + incFormulaID
	//incProcessID := modules.GenerateUUID()
	incTimeNow := DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	//incDatas := mapIncoming["datas"].([]map[string]interface{})
	rawDatas := mapIncoming["datas"].([]interface{})

	for _, data := range rawDatas {
		mapData := data.(map[string]interface{})
		var listDelKey []string
		//cleanMapData := make(map[string]interface{})
		for key, obj := range mapData {
			cleanKey := strings.ReplaceAll(key, " ", "")
			if cleanKey != key {
				listDelKey = append(listDelKey, key)
				mapData[cleanKey] = obj
			}
		}
		if len(listDelKey) > 0 {
			for _, key := range listDelKey {
				delete(mapData, key)
			}
		}

		incDatas = append(incDatas, mapData)
	}
	names, err := db.ListCollectionNames(cx, bson.M{})
	if err != nil {
		return false
	}
	isCollExist := false
	for _, name := range names {
		if name == collectionName {
			isCollExist = true
			break
		}
	}
	if !isCollExist {
		command := bson.D{{"create", collectionName}}
		var result bson.M
		if errDB := db.RunCommand(context.TODO(), command).Decode(&result); err != nil {
			panic(errDB)
		}
	}
	collection := db.Collection(collectionName)

	for i, data := range incDatas {
		incDataID := GetStringFromMapInterface(data, "dataid")
		println(fmt.Sprintf("incDataID: %+v", incDataID))
		if !(len(incDataID) > 0) {
			count := i
			if i > 10 {
				count = i - 10
			}
			strCount := strconv.Itoa(count)
			datemili := DoFormatDateTime("YY0M0DHHmmssS", time.Now())
			incDataID = strings.ToUpper(incFormulaID) + "_DATA" + datemili + strCount
			println(fmt.Sprintf("incDataID: %+v, datemili: %+v", incDataID, datemili))
		}
		data["data_id"] = incDataID
		data["formula_id"] = incFormulaID
		data["client_id"] = incClientID
		data["data_receive_datetime"] = incTimeNow
		one, errIns := collection.InsertOne(cx, data)
		if errIns != nil {
			return false
		}
		println(fmt.Sprintf("insert one: %+v", one))
		//jsonData := modules.ConvertMapInterfaceToJSON(data)

	}
	return true
}

func SaveFormulaBillingIntoPg(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (bool, string) {

	isSuccess := false

	incClientID := GetStringFromMapInterface(mapIncoming, "clientid")

	//incFields := mapIncoming["fields"].(map[string]interface{})
	//jsonFields := modules.ConvertMapInterfaceToJSON(incFields)

	incFields := mapIncoming["fields"].(interface{})

	incFormula := GetStringFromMapInterface(mapIncoming, "formula")
	incFormulaName := GetStringFromMapInterface(mapIncoming, "name")
	incType := GetStringFromMapInterface(mapIncoming, "type")
	incTime := GetStringFromMapInterface(mapIncoming, "time")
	incFormulaID := GenerateFormulaID(incClientID, incFormulaName)
	incTimeNow := DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	if strings.ToUpper(incType) == "REALTIME" {
		incTime = "00:00"
	}

	query := `INSERT INTO yformula_v3 (formula_id, client_id, formula_name,	fields, formula, formula_type, formula_time,
        formula_create_datetime, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`

	result, err := db.Exec(query, incFormulaID, incClientID, incFormulaName, pq.Array(incFields),
		incFormula, incType, incTime, incTimeNow, true)

	if err != nil {
		DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to insert tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			isSuccess = true
			DoLog("INFO", incTraceCode, "API", "Formula",
				"Success to insert tables.", true, nil)
		} else {
			isSuccess = false
			DoLog("ERROR", incTraceCode, "API", "Formula",
				"Failed to insert tables. Error occur.", true, err)
		}
	}
	return isSuccess, incFormulaID
}

func SaveFormulaArrayBillingIntoPg(db *sql.DB, incTraceCode string, mapIncoming map[string]interface{}) (bool, string, string) {

	isSuccess := false
	strFormula := ""

	incClientID := GetStringFromMapInterface(mapIncoming, "clientid")

	incFields := mapIncoming["fields"].([]interface{})
	var arrFields []string

	for _, incField := range incFields {
		arrFields = append(arrFields, strings.ReplaceAll(incField.(string), " ", ""))
	}

	//interfaceFields := incFields.(interface{})

	isFormulaValid := true
	incFormulaMap := mapIncoming["formula"].(map[string]interface{})
	for key, incItem := range incFormulaMap {
		arrItem := incItem.([]interface{})
		for _, item := range arrItem {
			strItem := item.(string)
			splitStrItem := strings.Split(strItem, "=")
			if len(splitStrItem) > 1 {
				DoLog("INFO", incTraceCode, "API", "Formula",
					fmt.Sprintf("splitStrItem: %+v.", splitStrItem), false, nil)
				DoLog("INFO", incTraceCode, "API", "Formula",
					fmt.Sprintf("splitStrItem[1]: %+v, splitStrItem[0]: %+v.", splitStrItem[1], splitStrItem[0]), false, nil)
				if strings.Contains(strings.ReplaceAll(splitStrItem[1], " ", ""), strings.ReplaceAll(splitStrItem[0], " ", "")) {
					DoLog("INFO", incTraceCode, "API", "Formula",
						fmt.Sprintf("TRUE: plitStrItem[1]: %+v, splitStrItem[0]: %+v.", strings.ReplaceAll(splitStrItem[1], " ", ""),
							strings.ReplaceAll(splitStrItem[0], " ", "")), false, nil)
					isFormulaValid = false
					break
				} else {
					strItem = strings.ReplaceAll(splitStrItem[0], " ", "") + " = " + strings.ReplaceAll(splitStrItem[1], " ", "")
				}
			}
			if strings.ToUpper(key) == "OUTPUTHEADER" || strings.ToUpper(key) == "OUTPUTRECAPGROUP" || strings.ToUpper(key) == "OUTPUTDATAGROUP" {
				arrStrItem := strings.Split(strItem, ",")
				newStrItem := ""

				for z := 0; z < len(arrStrItem); z++ {
					rawStrItem := ""
					if strings.Contains(arrStrItem[z], "asc") || strings.Contains(arrStrItem[z], "desc") {
						if strings.Contains(arrStrItem[z], "ASC") {
							tempStrItem := strings.TrimRight(arrStrItem[z], "asc")
							rawStrItem = strings.ReplaceAll(tempStrItem, " ", "") + " asc"
						} else if strings.Contains(arrStrItem[z], "desc") {
							tempStrItem := strings.TrimRight(arrStrItem[z], "desc")
							rawStrItem = strings.ReplaceAll(tempStrItem, " ", "") + " desc"
						}
					} else {
						rawStrItem = strings.ReplaceAll(arrStrItem[z], " ", "")
					}
					newStrItem += rawStrItem + ", "
					if z == len(arrStrItem)-1 {
						newStrItem = strings.TrimRight(newStrItem, ", ")
					}
				}
				if len(newStrItem) > 0 {
					strItem = newStrItem
				}
				strFormula += "@" + strings.ReplaceAll(key, " ", "") + ": " + strItem + "@\n"
			} else {
				strFormula += "@" + strings.ReplaceAll(key, " ", "") + ": " + strings.ReplaceAll(strItem, " ", "") + "@\n"
			}
			strFormula = strings.ReplaceAll(strFormula, "%", "/100")
		}
		if !isFormulaValid {
			break
		}
	}

	if !isFormulaValid {
		DoLog("ERROR", incTraceCode, "API", "Formula",
			"Formula Format is Invalid.", true, nil)
		return false, "", "Formula format is invalid"
	}

	if strings.HasSuffix(strFormula, "\n") {
		strings.TrimSuffix(strFormula, "\n")
	}
	incFormulaName := GetStringFromMapInterface(mapIncoming, "name")
	incType := GetStringFromMapInterface(mapIncoming, "type")
	incTime := GetStringFromMapInterface(mapIncoming, "time")
	incFormulaID := GenerateFormulaID(incClientID, incFormulaName)
	incTimeNow := DoFormatDateTime("YYYY-0M-0D HH:mm:ss.S", time.Now())

	if strings.ToUpper(incType) == "REALTIME" {
		incTime = "00:00"
	}

	query := `INSERT INTO yformula_v3 (formula_id, client_id, formula_name,	fields, formula, formula_type, formula_time,
        formula_create_datetime, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`

	result, err := db.Exec(query, incFormulaID, incClientID, incFormulaName, pq.Array(arrFields),
		strFormula, incType, incTime, incTimeNow, true)

	statusDesc := "Failed to insert tables"
	if err != nil {
		DoLog("ERROR", incTraceCode, "API", "Formula",
			"Failed to insert tables. Error occur.", true, err)
	} else {
		// Success
		rowAffected, _ := result.RowsAffected()
		if rowAffected >= 0 {
			isSuccess = true
			statusDesc = "Success to insert"
			DoLog("INFO", incTraceCode, "API", "Formula",
				"Success to insert tables.", true, nil)
		} else {
			DoLog("ERROR", incTraceCode, "API", "Formula",
				"Failed to insert tables. Error occur.", true, err)
		}
	}
	return isSuccess, incFormulaID, statusDesc
}

func GetSUM(incFormula string, mapDatas []map[string]interface{}) float64 {
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

func GetAVG(incFormula string, mapDatas []map[string]interface{}) float64 {
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

func GetCOUNT(incFormula string, mapDatas []map[string]interface{}) float64 {
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

func GetMAX(incFormula string, mapDatas []map[string]interface{}) float64 {
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

func GetMIN(incFormula string, mapDatas []map[string]interface{}) float64 {
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
