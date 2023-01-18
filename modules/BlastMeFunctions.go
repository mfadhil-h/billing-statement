package modules

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/streadway/amqp"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cornelk/hashmap"
	"github.com/go-redis/redis/v8"
	"golang.org/x/crypto/bcrypt"
)

var NDCHongkong = []string{"906", "914", "920", "934", "951", "958", "960", "971", "979", "606", "609", "615", "622", "623", "635", "643", "645", "648", "657", "664", "667",
	"670", "673", "674", "676", "684", "685", "687", "691", "693", "694", "695", "510", "512", "513", "516", "517", "531", "537", "539", "530", "534",
	"621", "658", "699", "544", "548", "549", "542", "562", "561", "560", "564", "598", "593", "553", "551", "557", "526", "522", "5283", "570", "574",
	"4648", "9290", "462", "636", "514", "518", "523", "536", "541", "558", "590", "594", "612", "618", "625", "630", "638", "644", "649", "651", "660",
	"662", "669", "680", "692", "907", "923", "927", "952", "964", "969", "977", "980", "511", "514", "521", "524", "527", "528", "529", "540", "543",
	"547", "552", "554", "568", "571", "579", "591", "592", "599", "602", "603", "605", "613", "614", "616", "617", "628", "629", "637", "639", "641",
	"642", "653", "654", "659", "665", "671", "675", "677", "682", "689", "697", "902", "903", "908", "909", "910", "915", "918", "919", "921", "925",
	"926", "930", "933", "940", "946", "953", "955", "961", "962", "965", "976", "978", "987", "988", "511", "514", "520", "521", "524", "527", "528",
	"529", "532", "540", "543", "547", "552", "554", "568", "570", "571", "579", "591", "592", "599", "602", "603", "605", "613", "614", "616", "617",
	"628", "629", "636", "637", "639", "641", "642", "653", "654", "659", "665", "671", "675", "677", "682", "689", "697", "902", "903", "908", "909",
	"910", "915", "918", "919", "921", "923", "925", "926", "930", "933", "940", "946", "953", "955", "961", "962", "965", "976", "978", "987", "988",
	"4610", "4611", "4613", "4614", "4640", "4647", "5749", "6362", "8481", "6454", "4655", "4656", "4657", "4658", "7080", "7081", "7082", "7083", "7084",
	"5149", "6040", "6041", "6042", "6043", "6044", "607", "608", "610", "619", "620", "640", "647", "650", "679", "904", "912", "928", "932", "935",
	"936", "937", "941", "942", "947", "948", "949", "950", "957", "963", "967", "974", "975", "981", "983", "5289", "533", "546", "556", "563", "566",
	"569", "596", "597", "6045", "6046", "6047", "6048", "6049", "631", "633", "634", "668", "690", "519", "970", "982", "6260", "601", "611", "632",
	"646", "901", "913", "916", "917", "922", "924", "931", "938", "943", "944", "945", "954", "966", "968", "972", "973", "984", "985", "986", "559",
	"550", "523", "8329", "528", "574", "572", "464", "707", "462", "848"}

var NDCMalaysia = []string{"19", "13", "148", "145", "1030", "1031", "1032", "1033", "1034", "1040", "1041", "1050", "1051", "1052", "1053", "1054", "1057", "1058", "1059",
	"1077", "1078", "1079", "1080", "1081", "1083", "1084", "1085", "1086", "1087", "11105", "11106", "11107", "11108", "11109", "11130", "11131", "11132", "11133", "11134",
	"11145", "11146", "11147", "11148", "11149", "1115", "11185", "11186", "11187", "11188", "11189", "1119", "11205", "11206", "11207", "11208", "11209", "1129", "11320",
	"11321", "11322", "11323", "11324", "11325", "11326", "11327", "11329", "1135", "1140", "1141", "1152", "1592", "14400", "11245", "11246", "11247", "11248", "11249",
	"11560", "11561", "11562", "11563", "11564", "1154", "1155", "11530", "11535", "11565", "11575", "11585", "11595", "11566", "11587", "11589", "11577", "11567", "11568",
	"11569", "11576", "11578", "11579", "11586", "11588", "11596", "11597", "11598", "11599", "11531", "11532", "11533", "11534", "11536", "11537", "11538", "11539", "11328", "16",
	"102", "143", "146", "149", "1036", "1037", "1038", "1039", "1046", "1056", "1066", "1076", "1082", "1088", "1091", "1092", "1093", "1094", "1095", "1096", "1097", "1098",
	"1116", "1122", "1126", "1131", "1133", "1136", "1150", "1151", "1591", "10900", "10901", "10902", "10903", "10905", "10906", "10907", "10908", "10909", "11200", "11201",
	"11202", "11203", "11204", "11590", "11646", "11647", "11648", "11692", "11703", "11704", "11705", "11706", "11707", "11708", "11709", "11710", "11711", "15960", "15961",
	"15962", "15963", "15964"}

var NDCIndonesia = []string{"814", "815", "816", "855", "856", "857", "858", "814", "815", "816", "855", "856", "857", "858", "814", "815", "816", "855", "856", "857", "858",
	"817", "818", "819", "859", "878", "877", "831", "838", "833", "832", "811", "812", "813", "852", "853", "821", "822", "823", "851", "881", "882", "888", "889", "895", "896",
	"897", "898", "899"}

var NDCPhilippines = []string{"905", "906", "915", "916", "917", "926", "927", "935", "936", "937", "945", "955", "956", "965", "966", "967", "975", "976", "977", "995", "996",
	"997", "952", "953", "954", "957", "959", "971", "972", "978", "922", "923", "925", "932", "933", "942", "943", "813", "907", "908", "909", "910", "912", "918", "919", "920",
	"921", "922", "923", "925", "928", "929", "930", "931", "932", "933", "938", "939", "942", "943", "946", "947", "948", "949", "950", "962", "981", "998", "999", "951", "960",
	"961", "968", "958", "982", "963"}

var NDCSingapore = []string{"812", "826", "828", "830", "831", "842", "843", "845", "873", "901", "903", "905", "911", "912", "913", "915", "917", "923", "935", "937", "939",
	"944", "946", "971", "972", "973", "975", "977", "978", "981", "986", "989", "891"}

var NDCLaos = []string{"202", "302", "2050", "2051", "2052", "2053", "2054", "2055", "2056", "2057", "2058", "2059", "305", "209", "309", "304"}

func InitiateClientGroupMultiRouter20(db *sql.DB) {
	query := "select client_group_id, incoming_queue, tps_router_speed from client_group_multiroute"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateClientGroupMultiRouter20",
			"Failed to read database client group and multirouter mapping. Error occured.", true, err)
	} else {
		defer rows.Close()

		// Delete old records
		RedisDelKeysWithPatternRedis("", "clientGroupMultiRoute20-*")

		for rows.Next() {
			var clientGroupId string
			var incomingQueue string
			var tpsRouterSpeed int

			err = rows.Scan(&clientGroupId, &incomingQueue, &tpsRouterSpeed)

			if err != nil {
				// Error scan table countryCodePrefix
				DoLog("INFO", "", "BlastMeFunctions", "InitiateClientGroupMultiRouter20",
					"Failed to scan table client group multiroute. Error occured.", true, err)
			} else {
				var mapDetail = make(map[string]interface{})

				mapDetail["clientGroupId"] = clientGroupId
				mapDetail["incomingQueue"] = incomingQueue
				mapDetail["tpsRouterSpeed"] = tpsRouterSpeed

				// Convert mapDetail to json and put it into redis
				redisKey := "clientGroupMultiRoute20-" + clientGroupId
				redisVal := ConvertMapInterfaceToJSON(mapDetail)

				RedisSetDataRedis("", redisKey, redisVal)
				DoLog("INFO", "", "BlastMeFunctions", "InitiateClientGroupMultiRouter20",
					"Successfully published to redis clientGroupMultiRoute20 with redisKey: "+redisKey+" -> redisVal: "+redisVal, false, nil)
			}
		}
	}
}

func GetRouterQueueNameByClientGroupId20(messageId, clientGroupId string) (string, int) {
	queueName := ""
	tpsSpeed := 0

	redisKey := "clientGroupMultiRoute20-" + clientGroupId
	redisVal := RedisGetDataRedis(messageId, redisKey)

	if len(strings.TrimSpace(redisVal)) == 0 {
		// Use default clientGroupId
		redisKey = "clientGroupMultiRoute20-DEFAULT"
		redisVal = RedisGetDataRedis(messageId, redisKey)
	}
	DoLog("INFO", "", "BlastMeFunctions", "GetRouterQueueNameByClientGroupId20",
		"redisKey: "+redisKey+", redisVal: "+redisVal, false, nil)

	// Convert redisVal to map interface
	mapRedis := ConvertJSONStringToMap(messageId, redisVal)
	DoLog("INFO", "", "BlastMeFunctions", "GetRouterQueueNameByClientGroupId20",
		"redisKey: "+redisKey+", mapRedis: "+fmt.Sprintf("%+v", mapRedis), false, nil)

	queueName = GetStringFromMapInterface(mapRedis, "incomingQueue")
	tpsSpeed = int(mapRedis["tpsRouterSpeed"].(float64))

	return queueName, tpsSpeed
}

func InitiateCountryCodePrefix20(db *sql.DB) map[string]map[string]string {
	var mapReturn = make(map[string]map[string]string)

	query := "select country_code_and_prefix_id, country_code, telecom_id, prefix from country_code_prefix where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix",
			"Failed to read database Country Code and Prefix. Error occured.", true, err)
	} else {
		defer rows.Close()

		// Delete old records
		RedisDelKeysWithPatternRedis("", "countryCodePrefix20-*")

		for rows.Next() {
			var countryCodePrefxId string
			var countryCode string
			var telecomId string
			var prefix string

			err = rows.Scan(&countryCodePrefxId, &countryCode, &telecomId, &prefix)

			if err != nil {
				// Error scan table countryCodePrefix
				DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix20",
					"Failed to scan table country code prefix. Error occured.", true, err)
			} else {
				var mapDetail = make(map[string]string)

				mapDetail["countryCodePrefixId"] = countryCodePrefxId
				mapDetail["countryCode"] = countryCode
				mapDetail["telecomId"] = telecomId
				mapDetail["prefix"] = prefix

				// Put int mapReturn
				mapReturn[countryCodePrefxId] = mapDetail

				// Convert mapDetail to json and put it into redis
				//redisKey := "countryCodePrefix-" + countryCodePrefxId
				//redisVal := ConvertMapStringToJSON(mapDetail)
				//
				//RedisSetDataRedis("", redisKey, redisVal)
				//DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix20",
				//	"Successfully published to redis mapCountryCodePrefix with redisKey: "+redisKey+" -> redisVal: "+redisVal, false, nil)
			}
		}
	}

	return mapReturn
}

func InitiateCountryCodePrefixRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) map[string]map[string]string {
	var mapReturn = make(map[string]map[string]string)

	query := "select country_code_and_prefix_id, country_code, telecom_id, prefix from country_code_prefix where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix",
			"Failed to read database Country Code and Prefix. Error occured.", true, err)
	} else {
		defer rows.Close()

		// Delete all redis with key pattern
		redisKeyPattern := "countryCodePrefix-*"
		RedisDeleteKeysByPattern(redisClient, goContext, redisKeyPattern)

		for rows.Next() {
			var countryCodePrefxId string
			var countryCode string
			var telecomId string
			var prefix string

			err = rows.Scan(&countryCodePrefxId, &countryCode, &telecomId, &prefix)

			if err != nil {
				// Error scan table countryCodePrefix
				DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix20",
					"Failed to scan table country code prefix. Error occured.", true, err)
			} else {
				var mapDetail = make(map[string]string)

				mapDetail["countryCodePrefixId"] = countryCodePrefxId
				mapDetail["countryCode"] = countryCode
				mapDetail["telecomId"] = telecomId
				mapDetail["prefix"] = prefix

				// Put int mapReturn
				mapReturn[countryCodePrefxId] = mapDetail

				// Convert mapDetail to json and put it into memcached
				redisKey := "smsCountryCodePrefix-" + countryCodePrefxId
				redisVal := ConvertMapStringToJSON(mapDetail)

				errR := RedisSet(redisClient, goContext, redisKey, redisVal, 0)
				if errR != nil {
					DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix20",
						redisKey+" -> APIGetToken submit status: Failed. Error occur.", true, errR)
				} else {
					DoLog("INFO", "", "BlastMeFunctions", "InitiateCountryCodePrefix20",
						redisKey+" -> APIGetToken submit status: SUCCESS.", false, nil)
				}
			}
		}
	}

	return mapReturn
}

func GetAPICredential20(hashmapAPICredential *hashmap.HashMap, messageId string, username string) map[string]interface{} {
	mapAPICredential := make(map[string]interface{})

	rawMapCredential, ok := hashmapAPICredential.Get(username)

	if !ok {
		DoLog("DEBUG", messageId, "BlastMeFunction", "GetAPICredential20",
			"Failed to get map credential.", false, nil)
	} else {
		mapAPICredential = rawMapCredential.(map[string]interface{})
	}

	DoLog("DEBUG", messageId, "BlastMeFunction", "GetAPICredential20",
		fmt.Sprintf("mapAPICredential: %+v", mapAPICredential), false, nil)
	return mapAPICredential
}

func GetCountryCodePrefixValid20ByMap(mapCountryCodePrefix map[string]map[string]string, msisdn string) (bool, string, string, string) {
	isValid := false
	countryCode := ""
	prefix := ""
	telecomId := ""

	for countryCodePrefixId, mapDetail := range mapCountryCodePrefix {
		fmt.Println("- Checking MSISDN: " + strings.TrimSpace(msisdn) + ", countryCodePrefix: " + countryCodePrefixId)
		if strings.HasPrefix(strings.TrimSpace(msisdn), countryCodePrefixId) {
			isValid = true

			countryCode = mapDetail["countryCode"]
			prefix = mapDetail["prefix"]
			telecomId = mapDetail["telecomId"]

			break
		}
	}

	DoLog("DEBUG", "", "BlastMeFunctions", "GetCountryCodePrefixValid20ByMap",
		"msisdn: "+msisdn+" -> Final result - isValid: "+fmt.Sprintf("%t", isValid)+", countryCode: "+countryCode+
			", prefix: "+prefix+", telecomId: "+telecomId, false, nil)

	return isValid, countryCode, prefix, telecomId
}

func InitiateAPICredential(db *sql.DB) (*hashmap.HashMap, *hashmap.HashMap) {
	var mapReturnByUserName = &hashmap.HashMap{}
	var mapReturnByClient = &hashmap.HashMap{}

	query := "select username, password, client_id, access_type, registered_ip_address from user_api"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateAPICredential",
			"Failed to read API Credential data. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var username sql.NullString
			var password sql.NullString
			var clientId sql.NullString
			var accessType sql.NullString
			var registeredIPAddress sql.NullString

			err = rows.Scan(&username, &password, &clientId, &accessType, &registeredIPAddress)

			// Prepare map for redis
			var mapCredential = make(map[string]interface{})
			mapCredential["username"] = ConvertSQLNullStringToString(username)
			mapCredential["password"] = ConvertSQLNullStringToString(password)
			mapCredential["clientId"] = ConvertSQLNullStringToString(clientId)
			mapCredential["accessType"] = ConvertSQLNullStringToString(accessType)
			mapCredential["registeredIPAddress"] = ConvertSQLNullStringToString(registeredIPAddress)

			// Put into mapReturn by username
			mapReturnByUserName.Set(ConvertSQLNullStringToString(username), mapCredential)

			// Put into mapReturn by clientId - it there is more than one, the old record will be replaced by new latest read record
			mapReturnByClient.Set(ConvertSQLNullStringToString(clientId), mapCredential)
		}
	}

	return mapReturnByUserName, mapReturnByClient
}

func InitiateAPICredentialByUserNameAndClientToRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) {
	//var mapReturnByUserName = &hashmap.HashMap{}
	//var mapReturnByClient = &hashmap.HashMap{}
	theRedisClient := redisClient

	query := "select username, password, client_id, access_type, registered_ip_address from user_api WHERE is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateAPICredential",
			"Failed to read API Credential data. Error occured.", true, err)
	} else {
		RedisDeleteKeysByPattern(redisClient, goContext, "smsAPICredByUserName_*")
		RedisDeleteKeysByPattern(redisClient, goContext, "smsAPICredByClient_")
		defer rows.Close()

		for rows.Next() {
			var username sql.NullString
			var password sql.NullString
			var clientId sql.NullString
			var accessType sql.NullString
			var registeredIPAddress sql.NullString

			err = rows.Scan(&username, &password, &clientId, &accessType, &registeredIPAddress)

			// Prepare map for redis
			var mapCredential = make(map[string]interface{})
			mapCredential["username"] = ConvertSQLNullStringToString(username)
			mapCredential["password"] = ConvertSQLNullStringToString(password)
			mapCredential["clientId"] = ConvertSQLNullStringToString(clientId)
			mapCredential["accessType"] = ConvertSQLNullStringToString(accessType)
			mapCredential["registeredIPAddress"] = ConvertSQLNullStringToString(registeredIPAddress)

			// Put into mapReturn by username
			redisKeyUserName := "smsAPICredByUserName_" + ConvertSQLNullStringToString(username)
			redisValUserName := ConvertMapInterfaceToJSON(mapCredential)

			errR := RedisSet(theRedisClient, goContext, redisKeyUserName, redisValUserName, 0)
			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateAPICredential",
					redisKeyUserName+" -> Failed to set API Credential data to redis. Error occured.", true, errR)
			}

			// Put into mapReturn by clientId - it there is more than one, the old record will be replaced by new latest read record
			redisKeyClient := "smsAPICredByClient_" + ConvertSQLNullStringToString(clientId)
			redisValClient := ConvertMapInterfaceToJSON(mapCredential)
			errC := RedisSet(theRedisClient, goContext, redisKeyClient, redisValClient, 0)
			if errC != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateAPICredential",
					redisKeyClient+" -> Failed to set API Credential data to redis. Error occured.", true, errR)
			}
		}
	}
}

func GetWABAAPITokenData(redisClient *redis.Client, goContext context.Context,
	clientUserName string, clientID string) (bool, string, string, string) {
	// Prepare the response
	isValid := false
	token := ""
	username := ""
	remoteIPAddress := ""

	redisKey := "_tknwaba_" + clientUserName + "_" + clientID
	redisVal, err := RedisGet(redisClient, goContext, redisKey)

	if err != nil {
		isValid = false
	} else {
		mapRedis := ConvertJSONStringToMap("", redisVal)

		token = GetStringFromMapInterface(mapRedis, "token")
		username = GetStringFromMapInterface(mapRedis, "username")
		remoteIPAddress = GetStringFromMapInterface(mapRedis, "remoteipaddress")
		stringExpiry := GetStringFromMapInterface(mapRedis, "expiry")

		// Convert string Expiry to time.Time
		// stringExpiry format: YY-0M-0D HH:mm:ss
		strTimeLayout := "06-01-02 15:04:05"
		redisExpTime, errParse := time.Parse(strTimeLayout, stringExpiry)

		if errParse != nil {
			isValid = false
		} else {
			if time.Now().Before(redisExpTime) {
				isValid = true
			}
		}
	}

	return isValid, token, username, remoteIPAddress
}

func GetAPICredentialByUserNameFromRedis(redisClient *redis.Client, goContext context.Context, apiUserName string) map[string]interface{} {
	mapReturn := make(map[string]interface{})

	redisKey := "smsAPICredByUserName_" + apiUserName
	redisVal, err := RedisGet(redisClient, goContext, redisKey)

	if err == nil {
		mapReturn = ConvertJSONStringToMap("", redisVal)
	}

	return mapReturn
}

func GetAPICredentialByClientIdFromRedis(redisClient *redis.Client, goContext context.Context, clientId string) map[string]interface{} {
	mapReturn := make(map[string]interface{})

	redisKey := "smsAPICredByClient_" + clientId
	redisVal, err := RedisGet(redisClient, goContext, redisKey)

	if err == nil {
		mapReturn = ConvertJSONStringToMap("", redisVal)
	}

	return mapReturn
}

func InitiateEmailRoutingTable(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select routing_id, client_id, client_sender_id, vendor_id, vendor_sender_id, vendor_parameter_json, " +
		"client_price_per_submit, vendor_price_per_submit, client_currency_id, vendor_currency_id, fake_dr, client_user_api " +
		"from routing_table_email where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateEmailRoutingTable",
			"Failed to query to email routing table. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var routingId sql.NullString
			var clientId sql.NullString
			var clientSenderId sql.NullString
			var vendorId sql.NullString
			var vendorSenderId sql.NullString
			var vendorParameterJson sql.NullString
			var clientPricePerSubmit sql.NullFloat64
			var vendorPricePerSubmit sql.NullFloat64
			var clientCurrencyId sql.NullString
			var vendorCurrencyId sql.NullString
			var fakeDR sql.NullBool
			var clientUserAPI sql.NullString

			err = rows.Scan(&routingId, &clientId, &clientSenderId, &vendorId, &vendorSenderId, &vendorParameterJson, &clientPricePerSubmit,
				&vendorPricePerSubmit, &clientCurrencyId, &vendorCurrencyId, &fakeDR, &clientUserAPI)

			// Prepare map for redis
			var mapEmailRoutingTable = make(map[string]interface{})
			mapEmailRoutingTable["routingId"] = ConvertSQLNullStringToString(routingId)
			mapEmailRoutingTable["clientId"] = ConvertSQLNullStringToString(clientId)
			mapEmailRoutingTable["clientSenderId"] = ConvertSQLNullStringToString(clientSenderId)
			mapEmailRoutingTable["vendorId"] = ConvertSQLNullStringToString(vendorId)
			mapEmailRoutingTable["vendorSenderId"] = ConvertSQLNullStringToString(vendorSenderId)
			mapEmailRoutingTable["vendorParameterJSON"] = ConvertSQLNullStringToString(vendorParameterJson)
			mapEmailRoutingTable["clientPricePerSubmit"] = ConvertSQLNullFloat64ToFloat64(clientPricePerSubmit)
			mapEmailRoutingTable["vendorPricePerSubmit"] = ConvertSQLNullFloat64ToFloat64(vendorPricePerSubmit)
			mapEmailRoutingTable["clientCurrencyId"] = ConvertSQLNullStringToString(clientCurrencyId)
			mapEmailRoutingTable["vendorCurrencyId"] = ConvertSQLNullStringToString(vendorCurrencyId)
			mapEmailRoutingTable["fakeDR"] = ConvertSQLNullBoolToBool(fakeDR)
			mapEmailRoutingTable["clientUserAPI"] = ConvertSQLNullStringToString(clientUserAPI)

			// Put into mapReturn
			mapReturn.Set(ConvertSQLNullStringToString(routingId), mapEmailRoutingTable)
		}
	}

	return mapReturn
}

func InitiateRoutingTable20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select rts.routing_id, rts.client_id, clt.client_group_id, rts.client_sender_id_id, rts.telecom_id, rts.vendor_id, " +
		"rts.vendor_sender_id_id, rts.vendor_parameter_json, rts.client_price_per_submit, rts.vendor_price_per_submit, rts.fake_dr, " +
		"rts.client_user_api, rts.is_charged_per_dr, rts.client_price_per_delivery, rts.vendor_price_per_delivery, rts.voice_unit_second, " +
		"rts.voice_price_per_unit, clt.business_model, clt.currency_id from routing_table_sms as rts left join client as clt on " +
		"rts.client_id = clt.client_id where clt.is_active = true and rts.is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateRoutingTable20",
			"Failed to query to table routing table. Error occured.", true, err)
	} else {
		for rows.Next() {
			var routingId sql.NullString
			var clientId sql.NullString
			var clientGroupId sql.NullString
			var clientSenderIdId sql.NullString
			var telecomId sql.NullString
			var vendorId sql.NullString
			var vendorSenderIdId sql.NullString
			var vendorParameterJSON sql.NullString
			var clientPricePerSubmit sql.NullFloat64
			var vendorPricePerSubmit sql.NullFloat64
			var isFakeDr sql.NullBool
			var clientUserApi sql.NullString
			var isChargedPerDr sql.NullBool
			var clientPricePerDelivery sql.NullFloat64
			var vendorPricePerDelivery sql.NullFloat64
			var voiceUnitInSecond sql.NullInt64
			var voicePricePerUnit sql.NullFloat64
			var businessModel sql.NullString
			var currencyId sql.NullString

			err = rows.Scan(&routingId, &clientId, &clientGroupId, &clientSenderIdId, &telecomId, &vendorId, &vendorSenderIdId, &vendorParameterJSON,
				&clientPricePerSubmit, &vendorPricePerSubmit, &isFakeDr, &clientUserApi, &isChargedPerDr, &clientPricePerDelivery,
				&vendorPricePerDelivery, &voiceUnitInSecond, &voicePricePerUnit, &businessModel, &currencyId)

			// Prepare map for redis
			var mapRedisRoutingTable = make(map[string]interface{})
			mapRedisRoutingTable["routingId"] = ConvertSQLNullStringToString(routingId)
			mapRedisRoutingTable["clientId"] = ConvertSQLNullStringToString(clientId)
			mapRedisRoutingTable["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
			mapRedisRoutingTable["clientSenderIdId"] = ConvertSQLNullStringToString(clientSenderIdId)
			mapRedisRoutingTable["telecomId"] = ConvertSQLNullStringToString(telecomId)
			mapRedisRoutingTable["vendorId"] = ConvertSQLNullStringToString(vendorId)
			mapRedisRoutingTable["vendorSenderIdId"] = ConvertSQLNullStringToString(vendorSenderIdId)
			mapRedisRoutingTable["vendorParameterJSON"] = ConvertSQLNullStringToString(vendorParameterJSON)
			mapRedisRoutingTable["clientPricePerSubmit"] = ConvertSQLNullFloat64ToFloat64(clientPricePerSubmit)
			mapRedisRoutingTable["vendorPricePerSubmit"] = ConvertSQLNullFloat64ToFloat64(vendorPricePerSubmit)
			mapRedisRoutingTable["isFakeDR"] = ConvertSQLNullBoolToBool(isFakeDr)
			mapRedisRoutingTable["clientUserAPI"] = ConvertSQLNullStringToString(clientUserApi)
			mapRedisRoutingTable["isChargedPerDR"] = ConvertSQLNullBoolToBool(isChargedPerDr)
			mapRedisRoutingTable["clientPricePerDelivery"] = ConvertSQLNullFloat64ToFloat64(clientPricePerDelivery)
			mapRedisRoutingTable["vendorPricePerDelivery"] = ConvertSQLNullFloat64ToFloat64(vendorPricePerDelivery)
			mapRedisRoutingTable["voiceUnitInSecond"] = ConvertSQLNullInt64ToInt64(voiceUnitInSecond)
			mapRedisRoutingTable["voicePricePerUnit"] = ConvertSQLNullFloat64ToFloat64(voicePricePerUnit)
			mapRedisRoutingTable["businessModel"] = ConvertSQLNullStringToString(businessModel)
			mapRedisRoutingTable["currencyId"] = ConvertSQLNullStringToString(currencyId)

			//DoLog("INFO", "","BlastMeFunctions", "InitiateClient20",
			//	"routingId: " + ConvertSQLNullStringToString(routingId) + " -> vendorParamter: " +
			//		ConvertSQLNullStringToString(vendorParameterJSON), true, err)

			// Put into mapReturn
			mapReturn.Set(ConvertSQLNullStringToString(routingId), mapRedisRoutingTable)
		}

		rows.Close()
	}

	return mapReturn
}

func InitiateRoutingTableRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) {
	query := "select rts.routing_id, rts.client_id, clt.client_group_id, rts.client_sender_id_id, rts.telecom_id, rts.vendor_id, " +
		"rts.vendor_sender_id_id, rts.vendor_parameter_json, rts.client_price_per_submit, rts.vendor_price_per_submit, rts.fake_dr, " +
		"rts.client_user_api, rts.is_charged_per_dr, rts.client_price_per_delivery, rts.vendor_price_per_delivery, rts.voice_unit_second, " +
		"rts.voice_price_per_unit, clt.business_model, clt.currency_id from routing_table_sms as rts left join client as clt on " +
		"rts.client_id = clt.client_id where clt.is_active = true and rts.is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateRoutingTable20",
			"Failed to query to table routing table. Error occured.", true, err)
	} else {
		RedisDeleteKeysByPattern(redisClient, goContext, "smsrouting_*")

		for rows.Next() {
			var routingId sql.NullString
			var clientId sql.NullString
			var clientGroupId sql.NullString
			var clientSenderIdId sql.NullString
			var telecomId sql.NullString
			var vendorId sql.NullString
			var vendorSenderIdId sql.NullString
			var vendorParameterJSON sql.NullString
			var clientPricePerSubmit sql.NullFloat64
			var vendorPricePerSubmit sql.NullFloat64
			var isFakeDr sql.NullBool
			var clientUserApi sql.NullString
			var isChargedPerDr sql.NullBool
			var clientPricePerDelivery sql.NullFloat64
			var vendorPricePerDelivery sql.NullFloat64
			var voiceUnitInSecond sql.NullInt64
			var voicePricePerUnit sql.NullFloat64
			var businessModel sql.NullString
			var currencyId sql.NullString

			err = rows.Scan(&routingId, &clientId, &clientGroupId, &clientSenderIdId, &telecomId, &vendorId, &vendorSenderIdId, &vendorParameterJSON,
				&clientPricePerSubmit, &vendorPricePerSubmit, &isFakeDr, &clientUserApi, &isChargedPerDr, &clientPricePerDelivery,
				&vendorPricePerDelivery, &voiceUnitInSecond, &voicePricePerUnit, &businessModel, &currencyId)

			// Prepare map for redis
			var mapRedisRoutingTable = make(map[string]interface{})
			mapRedisRoutingTable["routingId"] = ConvertSQLNullStringToString(routingId)
			mapRedisRoutingTable["clientId"] = ConvertSQLNullStringToString(clientId)
			mapRedisRoutingTable["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
			mapRedisRoutingTable["clientSenderIdId"] = ConvertSQLNullStringToString(clientSenderIdId)
			mapRedisRoutingTable["telecomId"] = ConvertSQLNullStringToString(telecomId)
			mapRedisRoutingTable["vendorId"] = ConvertSQLNullStringToString(vendorId)
			mapRedisRoutingTable["vendorSenderIdId"] = ConvertSQLNullStringToString(vendorSenderIdId)
			mapRedisRoutingTable["vendorParameterJSON"] = ConvertSQLNullStringToString(vendorParameterJSON)
			mapRedisRoutingTable["clientPricePerSubmit"] = ConvertSQLNullFloat64ToFloat64(clientPricePerSubmit)
			mapRedisRoutingTable["vendorPricePerSubmit"] = ConvertSQLNullFloat64ToFloat64(vendorPricePerSubmit)
			mapRedisRoutingTable["isFakeDR"] = ConvertSQLNullBoolToBool(isFakeDr)
			mapRedisRoutingTable["clientUserAPI"] = ConvertSQLNullStringToString(clientUserApi)
			mapRedisRoutingTable["isChargedPerDR"] = ConvertSQLNullBoolToBool(isChargedPerDr)
			mapRedisRoutingTable["clientPricePerDelivery"] = ConvertSQLNullFloat64ToFloat64(clientPricePerDelivery)
			mapRedisRoutingTable["vendorPricePerDelivery"] = ConvertSQLNullFloat64ToFloat64(vendorPricePerDelivery)
			mapRedisRoutingTable["voiceUnitInSecond"] = ConvertSQLNullInt64ToInt64(voiceUnitInSecond)
			mapRedisRoutingTable["voicePricePerUnit"] = ConvertSQLNullFloat64ToFloat64(voicePricePerUnit)
			mapRedisRoutingTable["businessModel"] = ConvertSQLNullStringToString(businessModel)
			mapRedisRoutingTable["currencyId"] = ConvertSQLNullStringToString(currencyId)

			//DoLog("INFO", "","BlastMeFunctions", "InitiateClient20",
			//	"routingId: " + ConvertSQLNullStringToString(routingId) + " -> vendorParamter: " +
			//		ConvertSQLNullStringToString(vendorParameterJSON), true, err)

			// Put into mapReturn
			//mapReturn.Set(ConvertSQLNullStringToString(routingId), mapRedisRoutingTable)

			// Put into redis
			redisKey := "smsrouting_" + ConvertSQLNullStringToString(routingId)
			redisVal := ConvertMapInterfaceToJSON(mapRedisRoutingTable)

			errR := RedisSet(redisClient, goContext, redisKey, redisVal, 0)
			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateRoutingTable20",
					redisKey+" -> Failed to write to redis. Error occured.", true, errR)
			} else {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateRoutingTable20",
					redisKey+" -> Success to write to redis.", false, nil)
			}
		}

		rows.Close()
	}
}

func GetRoutingDataFromRedis(redisClient *redis.Client, goContext context.Context, routingId string) map[string]interface{} {
	mapReturn := make(map[string]interface{})
	redisKey := "smsrouting_" + routingId
	redisVal, err := RedisGet(redisClient, goContext, redisKey)

	if err != nil {
		// Error
		DoLog("INFO", "", "BlastMeFunctions", "getRoutingDataFromRedis",
			redisKey+" -> Failed to read from redis. Error occured.", true, err)
	} else {
		// No error
		mapReturn = ConvertJSONStringToMap("", redisVal)
	}

	return mapReturn
}

func InitiateWATemplate20(db *sql.DB) *hashmap.HashMap {
	mapReturn := &hashmap.HashMap{}

	query := "select id, client_id, template_id, parameter_count, template_content, template_namespace, template_name, " +
		"template_type, template_lang_code, template_lang_policy, template_media_type " +
		"from wa_template where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateRoutingTable20",
			"Failed to query to table routing table. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var theId sql.NullString
			var clientId sql.NullString
			var templateId sql.NullString
			var parameterCount sql.NullInt64
			var templateContent sql.NullString
			var templateNameSpace sql.NullString
			var templateName sql.NullString
			var templateType sql.NullString
			var templateLangCode sql.NullString
			var templateLangPolicy sql.NullString
			var templateMediaType sql.NullString

			err = rows.Scan(&theId, &clientId, &templateId, &parameterCount, &templateContent, &templateNameSpace,
				&templateName, &templateType, &templateLangCode, &templateLangPolicy, &templateMediaType)

			var mapWATemplate = make(map[string]interface{})

			mapWATemplate["id"] = ConvertSQLNullStringToString(theId)
			mapWATemplate["clientId"] = ConvertSQLNullStringToString(clientId)
			mapWATemplate["templateId"] = ConvertSQLNullStringToString(templateId)
			mapWATemplate["parameterCount"] = ConvertSQLNullInt64ToInt64(parameterCount)
			mapWATemplate["templateContent"] = ConvertSQLNullStringToString(templateContent)
			mapWATemplate["templateNameSpace"] = ConvertSQLNullStringToString(templateNameSpace)
			mapWATemplate["templateName"] = ConvertSQLNullStringToString(templateName)
			mapWATemplate["templateType"] = ConvertSQLNullStringToString(templateType)
			mapWATemplate["templateLangCode"] = ConvertSQLNullStringToString(templateLangCode)
			mapWATemplate["templateLangPolicy"] = ConvertSQLNullStringToString(templateLangPolicy)
			mapWATemplate["templateMediaType"] = ConvertSQLNullStringToString(templateMediaType)

			// Put into mapReturn
			mapReturn.Set(ConvertSQLNullStringToString(theId), mapWATemplate)
		}
	}

	return mapReturn
}

func IsValidRouting20(hashmapRoutingTable *hashmap.HashMap, messageId string, clientSenderId string, clientId string, username string, telecomId string) bool {
	isValid := false

	routingId := strings.TrimSpace(clientSenderId) + "-" + strings.TrimSpace(clientId) + "-" + strings.TrimSpace(username) + "-" + strings.TrimSpace(telecomId)
	DoLog("INFO", "", "BlastMeFunctions", "IsValidRouting20",
		"isValidRouting20 - routingId: "+routingId, false, nil)

	rawMapRouting, ok := hashmapRoutingTable.Get(routingId)

	if !ok {
		isValid = false
	} else {
		mapRouting := rawMapRouting.(map[string]interface{})

		if mapRouting != nil && len(mapRouting) > 0 {
			isValid = true
		} else {
			isValid = false
		}
	}

	DoLog("INFO", "", "BlastMeFunctions", "IsValidRouting20",
		fmt.Sprintf("isValidRouting20 - isValid: %t", isValid), false, nil)

	return isValid
}

// get message type wadyn by vendorid
func GetMesageType(hashmapRoutingTable *hashmap.HashMap, messageId string, clientSenderId string, clientId string, username string, telecomId string) string {
	messageType := "TEXT"

	routingId := strings.TrimSpace(clientSenderId) + "-" + strings.TrimSpace(clientId) + "-" + strings.TrimSpace(username) + "-" + strings.TrimSpace(telecomId)
	DoLog("INFO", "", "BlastMeFunctions", "MessageTypeRouting20",
		"MessageTypeRouting20 - routingId: "+routingId, false, nil)

	rawMapRouting, ok := hashmapRoutingTable.Get(routingId)

	if !ok {
		messageType = "text"
	} else {
		mapRouting := rawMapRouting.(map[string]interface{})
		if mapRouting["vendorId"].(string) == "WAIA20220701" {
			messageType = "IMAGE"
		} else if mapRouting["vendorId"].(string) == "WAXT20220701" {
			messageType = "TEXT"
		} else if mapRouting["vendorId"].(string) == "WATP20220702" {
			messageType = "OTP"
		}
	}

	DoLog("INFO", "", "BlastMeFunctions", "MessageTypeRouting20",
		fmt.Sprintf("MessageTypeRouting20 - vendorId: %t", messageType), false, nil)

	return messageType
}

func IsValidRoutingRedis(messageId string, redisClient *redis.Client, goContext context.Context, clientSenderId string, clientId string, username string, telecomId string) bool {
	isValid := false

	routingId := strings.TrimSpace(clientSenderId) + "-" + strings.TrimSpace(clientId) + "-" + strings.TrimSpace(username) + "-" + strings.TrimSpace(telecomId)
	DoLog("INFO", messageId, "BlastMeFunctions", "IsValidRouting20",
		"isValidRouting20 - routingId: "+routingId, false, nil)

	//rawMapRouting, ok := hashmapRoutingTable.Get(routingId)
	mapRoutingTable := GetRoutingDataFromRedis(redisClient, goContext, routingId)

	if len(mapRoutingTable) > 0 {
		isValid = true
	} else {
		isValid = false
	}

	DoLog("INFO", "", "BlastMeFunctions", "IsValidRouting20",
		fmt.Sprintf("isValidRouting20 - isValid: %t", isValid), false, nil)

	return isValid
}

func InitiateAPICredential20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select upi.username, upi.password, upi.client_id, clt.client_group_id, upi.registered_ip_address, upi.access_type from user_api as upi " +
		"left join client as clt on upi.client_id = clt.client_id where upi.is_active = true and clt.is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateAPICredential20",
			"Failed to read table API credential. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var username sql.NullString
			var password sql.NullString
			var clientId sql.NullString
			var clientGroupId sql.NullString
			var registeredIpAddress sql.NullString
			var accessType sql.NullString

			err = rows.Scan(&username, &password, &clientId, &clientGroupId, &registeredIpAddress, &accessType)

			if err != nil {
				panic(err)
			} else {
				var mapRedisAPICredential = make(map[string]interface{})
				mapRedisAPICredential["username"] = ConvertSQLNullStringToString(username)
				mapRedisAPICredential["password"] = ConvertSQLNullStringToString(password)
				mapRedisAPICredential["clientId"] = ConvertSQLNullStringToString(clientId)
				mapRedisAPICredential["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
				mapRedisAPICredential["registeredIPAddress"] = ConvertSQLNullStringToString(registeredIpAddress)
				mapRedisAPICredential["accessType"] = ConvertSQLNullStringToString(accessType)

				mapReturn.Set(ConvertSQLNullStringToString(username), mapRedisAPICredential)
			}
		}
	}

	return mapReturn
}

func InitiateAPICredentialSMPPNEO(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select upi.username, upi.password, upi.client_id, clt.client_group_id, upi.registered_ip_address, upi.access_type from user_api as upi " +
		"left join client as clt on upi.client_id = clt.client_id where upi.is_active = true and clt.is_active = true and upi.access_type = 'NEOSMPPSMS'"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateAPICredential20",
			"Failed to read table API credential. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var username sql.NullString
			var password sql.NullString
			var clientId sql.NullString
			var clientGroupId sql.NullString
			var registeredIpAddress sql.NullString
			var accessType sql.NullString

			err = rows.Scan(&username, &password, &clientId, &clientGroupId, &registeredIpAddress, &accessType)

			if err != nil {
				panic(err)
			} else {
				var mapRedisAPICredential = make(map[string]interface{})
				mapRedisAPICredential["username"] = ConvertSQLNullStringToString(username)
				mapRedisAPICredential["password"] = ConvertSQLNullStringToString(password)
				mapRedisAPICredential["clientId"] = ConvertSQLNullStringToString(clientId)
				mapRedisAPICredential["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
				mapRedisAPICredential["registeredIPAddress"] = ConvertSQLNullStringToString(registeredIpAddress)
				mapRedisAPICredential["accessType"] = ConvertSQLNullStringToString(accessType)

				mapReturn.Set(ConvertSQLNullStringToString(username), mapRedisAPICredential)
			}
		}
	}

	return mapReturn
}

func InitiateClient20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select client_id, client_name, currency_id, business_model, client_group_id, upline_client_id from client where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateClient20",
			"Failed to read table client. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var clientId sql.NullString
			var clientName sql.NullString
			var currencyId sql.NullString
			var businessModel sql.NullString
			var clientGroupId sql.NullString
			var uplineClientId sql.NullString

			err = rows.Scan(&clientId, &clientName, &currencyId, &businessModel, &clientGroupId, &uplineClientId)

			// prepare map for redis
			var mapClient = make(map[string]interface{})
			mapClient["clientId"] = ConvertSQLNullStringToString(clientId)
			mapClient["clientName"] = ConvertSQLNullStringToString(clientName)
			mapClient["currencyId"] = ConvertSQLNullStringToString(currencyId)
			mapClient["businessModel"] = ConvertSQLNullStringToString(businessModel)
			mapClient["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
			mapClient["uplineClientId"] = ConvertSQLNullStringToString(uplineClientId)

			// Put into mapReturn
			mapReturn.Set(ConvertSQLNullStringToString(clientId), mapClient)
		}
	}

	return mapReturn
}

func InitiateVendorLimitRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) {
	query := "select vendor_limit_id, vendor_id, telecom_id, max_daily_traffix from vendor_limit where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateVendorLimitRedis",
			"Failed to read table vendor limit. Error occured.", true, err)
	} else {
		defer rows.Close()

		RedisDeleteKeysByPattern(redisClient, goContext, "vendorlimit_*")

		for rows.Next() {
			var vendorLimitId sql.NullString
			var vendorId sql.NullString
			var telecomId sql.NullString
			var maxDailyTraffix sql.NullInt64

			err = rows.Scan(&vendorLimitId, &vendorId, &telecomId, &maxDailyTraffix)

			// Defined redisKey
			redisKey := "vendorlimit_" + ConvertSQLNullStringToString(vendorLimitId)

			// Check if already set
			existingRedisVal, errExisting := RedisGet(redisClient, goContext, redisKey)

			if errExisting != nil || len(existingRedisVal) == 0 {
				// Maish belum di set
				mapRedisVal := make(map[string]interface{})
				mapRedisVal["counter"] = ConvertSQLNullInt64ToInt64(maxDailyTraffix)
				mapRedisVal["datetime"] = DoFormatDateTime("0M0D", time.Now())

				redisVal := ConvertMapInterfaceToJSON(mapRedisVal)

				errX := RedisSet(redisClient, goContext, redisKey, redisVal, 0)
				if errX != nil {
					DoLog("INFO", "", "BlastMeFunctions", "InitiateVendorLimitRedis",
						redisKey+" -> Failed to set to redis. Error occured.", true, errX)
				}
			} else {
				// Convert redisVal to map interface
				mapRedisVal := ConvertJSONStringToMap("", existingRedisVal)
				theDateTime := GetStringFromMapInterface(mapRedisVal, "datetime")

				if theDateTime != DoFormatDateTime("0M0D", time.Now()) {
					// Different date, update value
					mapRedisValS := make(map[string]interface{})
					mapRedisValS["counter"] = ConvertSQLNullInt64ToInt64(maxDailyTraffix)
					mapRedisValS["datetime"] = DoFormatDateTime("0M0D", time.Now())

					redisValS := ConvertMapInterfaceToJSON(mapRedisValS)

					errX := RedisSet(redisClient, goContext, redisKey, redisValS, 0)
					if errX != nil {
						DoLog("INFO", "", "BlastMeFunctions", "InitiateVendorLimitRedis",
							redisKey+" -> Failed to set to redis. Error occured.", true, errX)
					}
				} else {
					// Do nothing.
					DoLog("INFO", "", "BlastMeFunctions", "InitiateVendorLimitRedis",
						redisKey+" -> Counter is still running. Update the value manually if need. Can not do by initiating it.", false, nil)
				}
			}
		}
	}
}

func GetMaxVendorLimit(db *sql.DB, vendorId string, telecomId string) int64 {
	theMaxVendorLimit := int64(0)
	vendorLimitID := strings.TrimSpace(vendorId) + "_" + strings.TrimSpace(telecomId)

	query := "select max_daily_traffix from vendor_limit where is_active = true and vendor_limit_id = $1"

	rows, err := db.Query(query, vendorLimitID)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "GetMaxVendorLimit",
			"Failed to read table vendor limit. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var maxDailyTraffix sql.NullInt64

			err = rows.Scan(&maxDailyTraffix)

			if err != nil {
				theMaxVendorLimit = int64(0)
			} else {
				theMaxVendorLimit = ConvertSQLNullInt64ToInt64(maxDailyTraffix)
			}
		}
	}

	return theMaxVendorLimit
}

func GetCountVendorLimitAndUpdate(db *sql.DB, redisClient *redis.Client, goContext context.Context, vendorId string, telecomId string) int64 {
	remainCounter := int64(0)

	redisKey := "vendorlimit_" + strings.TrimSpace(vendorId) + "_" + strings.TrimSpace(telecomId)

	redisVal, errRedis := RedisGet(redisClient, goContext, redisKey)

	if errRedis != nil {
		DoLog("INFO", "", "BlastMeFunctions", "GetCountVendorLimitAndUpdate",
			redisKey+" -> Failed to read redis.", true, errRedis)

		remainCounter = int64(0)
	} else {
		// Convert redisVal JSON String to map interface
		mapRedisVal := ConvertJSONStringToMap("", redisVal)

		theCounter := GetInt64FromMapInterface(mapRedisVal, "counter")
		theDateTime := GetStringFromMapInterface(mapRedisVal, "datetime")

		// Check if last counter is today
		if theDateTime == DoFormatDateTime("0M0D", time.Now()) {
			// today counter countdown go
			newCounter := theCounter - 1

			mapNewRedisVal := make(map[string]interface{})
			mapNewRedisVal["counter"] = newCounter
			mapNewRedisVal["datetime"] = DoFormatDateTime("0M0D", time.Now())

			jsonNewRedisVal := ConvertMapInterfaceToJSON(mapNewRedisVal)

			errR := RedisSet(redisClient, goContext, redisKey, jsonNewRedisVal, 0)

			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "GetCountVendorLimitAndUpdate",
					redisKey+" -> Failed to set new value to redis.", true, errRedis)
			}

			remainCounter = newCounter
		} else {
			// counter is yesterday. Update today
			maxVendorLimit := GetMaxVendorLimit(db, vendorId, telecomId)

			// Update new value
			mapNewRedisVal := make(map[string]interface{})
			mapNewRedisVal["counter"] = maxVendorLimit
			mapNewRedisVal["datetime"] = DoFormatDateTime("0M0D", time.Now())

			jsonNewRedisVal := ConvertMapInterfaceToJSON(mapNewRedisVal)

			errR := RedisSet(redisClient, goContext, redisKey, jsonNewRedisVal, 0)

			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "GetCountVendorLimitAndUpdate",
					redisKey+" -> Failed to set new value to redis.", true, errRedis)
			}

			remainCounter = maxVendorLimit
		}
	}

	return remainCounter
}

func InitiateClientRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) {
	query := "select client_id, client_name, currency_id, business_model, client_group_id, upline_client_id from client where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateClient20",
			"Failed to read table client. Error occured.", true, err)
	} else {
		defer rows.Close()

		RedisDeleteKeysByPattern(redisClient, goContext, "smsclient_*")

		for rows.Next() {
			var clientId sql.NullString
			var clientName sql.NullString
			var currencyId sql.NullString
			var businessModel sql.NullString
			var clientGroupId sql.NullString
			var uplineClientId sql.NullString

			err = rows.Scan(&clientId, &clientName, &currencyId, &businessModel, &clientGroupId, &uplineClientId)

			// prepare map for redis
			var mapClient = make(map[string]interface{})
			mapClient["clientId"] = ConvertSQLNullStringToString(clientId)
			mapClient["clientName"] = ConvertSQLNullStringToString(clientName)
			mapClient["currencyId"] = ConvertSQLNullStringToString(currencyId)
			mapClient["businessModel"] = ConvertSQLNullStringToString(businessModel)
			mapClient["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
			mapClient["uplineClientId"] = ConvertSQLNullStringToString(uplineClientId)

			// Put into mapReturn
			//mapReturn.Set(ConvertSQLNullStringToString(clientId), mapClient)

			// Put into redis
			redisKey := "smsclient_" + ConvertSQLNullStringToString(clientId)
			redisVal := ConvertMapInterfaceToJSON(mapClient)

			errR := RedisSet(redisClient, goContext, redisKey, redisVal, 0)
			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateClient20",
					redisKey+" -> Failed to set to redis. Error occured.", true, errR)
			}
		}
	}
}

func GetClientDetailFromRedis(redisClient *redis.Client, goContext context.Context, clientId string) map[string]interface{} {
	mapReturn := make(map[string]interface{})

	redisKey := "smsclient_" + clientId
	redisVal, err := RedisGet(redisClient, goContext, redisKey)

	if err == nil {
		mapReturn = ConvertJSONStringToMap("", redisVal)
	} else {
		DoLog("INFO", "", "BlastMeFunctions", "GetClientDetailFromRedis",
			"Failed to read from redis for clientId: "+clientId, true, err)
	}

	return mapReturn
}

func InitiateVendor20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select vendor_id, vendor_name, queue_name, client_group_id from vendor_sms where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateVendor20",
			"Failed to read table vendor. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var vendorId sql.NullString
			var vendorName sql.NullString
			var queueName sql.NullString
			var clientGroupId sql.NullString

			err = rows.Scan(&vendorId, &vendorName, &queueName, &clientGroupId)

			// prepare mapRedisVendor
			var mapRedisVendor = make(map[string]interface{})
			mapRedisVendor["vendorId"] = ConvertSQLNullStringToString(vendorId)
			mapRedisVendor["vendorName"] = ConvertSQLNullStringToString(vendorName)
			mapRedisVendor["queueName"] = ConvertSQLNullStringToString(queueName)
			mapRedisVendor["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)
			//DoLog("INFO", "","BlastMeFunctions", "InitiateClient20",
			//	fmt.Sprintf("mapRedisVendor20: %+v", mapRedisVendor), false, nil)

			mapReturn.Set(ConvertSQLNullStringToString(vendorId), mapRedisVendor)
		}
	}

	return mapReturn
}

func InitiateVendorRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) {
	query := "select vendor_id, vendor_name, queue_name, client_group_id from vendor_sms where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateVendor20",
			"Failed to read table vendor. Error occured.", true, err)
	} else {
		defer rows.Close()

		RedisDeleteKeysByPattern(redisClient, goContext, "smsvendor_*")

		for rows.Next() {
			var vendorId sql.NullString
			var vendorName sql.NullString
			var queueName sql.NullString
			var clientGroupId sql.NullString

			err = rows.Scan(&vendorId, &vendorName, &queueName, &clientGroupId)

			// prepare mapRedisVendor
			var mapRedisVendor = make(map[string]interface{})
			mapRedisVendor["vendorId"] = ConvertSQLNullStringToString(vendorId)
			mapRedisVendor["vendorName"] = ConvertSQLNullStringToString(vendorName)
			mapRedisVendor["queueName"] = ConvertSQLNullStringToString(queueName)
			mapRedisVendor["clientGroupId"] = ConvertSQLNullStringToString(clientGroupId)

			redisKey := "smsvendor_" + ConvertSQLNullStringToString(vendorId)
			redisVal := ConvertMapInterfaceToJSON(mapRedisVendor)

			errR := RedisSet(redisClient, goContext, redisKey, redisVal, 0)
			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateVendor20",
					ConvertSQLNullStringToString(vendorId)+" -> Failed to write to redis. Error occur.", true, errR)
			}
		}
	}
}

func GetVendorDetailFromRedis(redisClient *redis.Client, goContext context.Context, vendorId string) map[string]interface{} {
	mapReturn := make(map[string]interface{})

	redisKey := "smsvendor_" + vendorId
	redisVal, err := RedisGet(redisClient, goContext, redisKey)

	if err == nil {
		mapReturn = ConvertJSONStringToMap("", redisVal)
	}

	return mapReturn
}

func InitiateEmailVendor(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select vendor_id, vendor_name, protocol, \"host\", \"port\", is_with_ssl, \"user\", \"password\", mail_type, charset, " +
		"timeout, wordwrap, transceiver_queue_name from vendor_mail"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateEmailVendor",
			"Failed to query table email vendor.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var vendorId sql.NullString
			var vendorName sql.NullString
			var protocol sql.NullString
			var host sql.NullString
			var port sql.NullInt64
			var isWithSSL sql.NullBool
			var user sql.NullString
			var password sql.NullString
			var mailType sql.NullString
			var charset sql.NullString
			var timeout sql.NullInt64
			var wordWrap sql.NullBool
			var transmitterQueueName sql.NullString

			errScan := rows.Scan(&vendorId, &vendorName, &protocol, &host, &port, &isWithSSL, &user, &password, &mailType,
				&charset, &timeout, &wordWrap, &transmitterQueueName)

			if errScan != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateEmailVendor",
					"Failed to scan table email vendor.", true, err)
			} else {
				// Success query and scan fields
				mapEmailVendor := make(map[string]interface{})
				mapEmailVendor["vendorId"] = ConvertSQLNullStringToString(vendorId)
				mapEmailVendor["vendorName"] = ConvertSQLNullStringToString(vendorName)
				mapEmailVendor["protocol"] = ConvertSQLNullStringToString(protocol)
				mapEmailVendor["host"] = ConvertSQLNullStringToString(host)
				mapEmailVendor["port"] = ConvertSQLNullInt64ToInt64(port)
				mapEmailVendor["isWithSSL"] = ConvertSQLNullBoolToBool(isWithSSL)
				mapEmailVendor["user"] = ConvertSQLNullStringToString(user)
				mapEmailVendor["password"] = ConvertSQLNullStringToString(password)
				mapEmailVendor["mailType"] = ConvertSQLNullStringToString(mailType)
				mapEmailVendor["charset"] = ConvertSQLNullStringToString(charset)
				mapEmailVendor["timeout"] = ConvertSQLNullInt64ToInt64(timeout)
				mapEmailVendor["wordWrap"] = ConvertSQLNullBoolToBool(wordWrap)
				mapEmailVendor["transmitterQueueName"] = ConvertSQLNullStringToString(transmitterQueueName)

				// Put into mapReturn
				mapReturn.Set(ConvertSQLNullStringToString(vendorId), mapEmailVendor)
			}
		}
	}

	return mapReturn
}

func InitiateTelinSMSAPIURL20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select api_username, telin_url from transceiver_telin_property where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateTelinSMSAPIURL20",
			"Failed to query table transceiver telin property.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var apiUserName string
			var telinUrl string

			errScan := rows.Scan(&apiUserName, &telinUrl)

			if errScan != nil {
				DoLog("INFO", "", "TelinSMS", "loadAccIdTokenIdRedis",
					"Failed to scan table transceiver telin property.", true, err)
			} else {
				// Success query and scan fields
				mapReturn.Set(apiUserName, telinUrl)
			}
		}
	}

	return mapReturn
}

func InitiateClientSenderId20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select client_sender_id_id, sender_id, client_id from client_senderid_sms where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateClientSenderId",
			"Failed to read database Country ClientSenderId. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var clientSenderIdId sql.NullString
			var senderId sql.NullString
			var clientId sql.NullString

			err = rows.Scan(&clientSenderIdId, &senderId, &clientId)

			var mapClientSenderIdDetail = make(map[string]string)

			mapClientSenderIdDetail["clientSenderIdId"] = ConvertSQLNullStringToString(clientSenderIdId)
			mapClientSenderIdDetail["senderId"] = ConvertSQLNullStringToString(senderId)
			mapClientSenderIdDetail["clientId"] = ConvertSQLNullStringToString(clientId)

			mapReturn.Set(ConvertSQLNullStringToString(clientSenderIdId), mapClientSenderIdDetail)
		}
	}

	return mapReturn
}

func InitiateClientSenderIdRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) {
	query := "select client_sender_id_id, sender_id, client_id from client_senderid_sms where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateClientSenderIdRedis",
			"Failed to read data clientSenderId. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var clientSenderIdId sql.NullString
			var senderId sql.NullString
			var clientId sql.NullString

			err = rows.Scan(&clientSenderIdId, &senderId, &clientId)

			var mapClientSenderIdDetail = make(map[string]string)

			mapClientSenderIdDetail["clientSenderIdId"] = ConvertSQLNullStringToString(clientSenderIdId)
			mapClientSenderIdDetail["senderId"] = ConvertSQLNullStringToString(senderId)
			mapClientSenderIdDetail["clientId"] = ConvertSQLNullStringToString(clientId)

			jsonClientSenderId := ConvertMapStringToJSON(mapClientSenderIdDetail)
			redisKey := ConvertSQLNullStringToString(clientSenderIdId)

			// Put into redis
			errR := RedisSet(redisClient, goContext, redisKey, jsonClientSenderId, 0)
			if errR != nil {
				DoLog("INFO", "", "BlastMeFunctions", "InitiateClientSenderIdRedis",
					redisKey+" -> Failed to write to redis. Error occured.", true, errR)
			}
		}
	}
}

func GetClientSenderIdRedis(messageId string, redisClient *redis.Client, goContext context.Context, clientId string, clientSenderId string) map[string]interface{} {
	mapClientSenderId := make(map[string]interface{})

	clientSenderIdId := strings.TrimSpace(clientSenderId) + "-" + strings.TrimSpace(clientId)

	redisVal, err := RedisGet(redisClient, goContext, clientSenderIdId)

	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "GetClientSenderIdRedis",
			"Failed to retrieve detail client sender id from redis. Error occur.", true, err)
	} else {
		mapClientSenderId = ConvertJSONStringToMap(messageId, redisVal)
	}

	return mapClientSenderId
}

func InitiateWAClientToQueueHandlerMapping20(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select client_id, queue_response_handler, queue_template_handler from wa_push_client_queue_handler"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateWAClientToQueueHandlerMapping20",
			"Failed to read database map client to queue handler. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var clientId sql.NullString
			var queueHandler sql.NullString
			var queueTemplateHandler sql.NullString

			err = rows.Scan(&clientId, &queueHandler, &queueTemplateHandler)

			var mapClientHandler = make(map[string]interface{})

			mapClientHandler["clientId"] = ConvertSQLNullStringToString(clientId)
			mapClientHandler["queueHandler"] = ConvertSQLNullStringToString(queueHandler)
			mapClientHandler["queueTemplateHandler"] = ConvertSQLNullStringToString(queueTemplateHandler)

			mapReturn.Set(ConvertSQLNullStringToString(clientId), mapClientHandler)
		}
	}

	return mapReturn
}

func InitiateEmailTemplate(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select id, client_id, template_id, html_template from email_template where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateEmailTemplate",
			"Failed to read database email template. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var theId sql.NullString // clientId + "-" + templateId with space replaced by _
			var clientId sql.NullString
			var templateId sql.NullString
			var htmlTemplate sql.NullString

			err = rows.Scan(&theId, &clientId, &templateId, &htmlTemplate)

			var mapEmailTemplate = make(map[string]interface{})

			mapEmailTemplate["theID"] = ConvertSQLNullStringToString(theId)
			mapEmailTemplate["clientId"] = ConvertSQLNullStringToString(clientId)
			mapEmailTemplate["templateId"] = ConvertSQLNullStringToString(templateId)
			mapEmailTemplate["htmlTemplate"] = ConvertSQLNullStringToString(htmlTemplate)

			mapReturn.Set(ConvertSQLNullStringToString(theId), mapEmailTemplate)
		}
	}

	return mapReturn
}

func InitiateEmailVendorSenderId(db *sql.DB) *hashmap.HashMap {
	var mapReturn = &hashmap.HashMap{}

	query := "select vendor_senderid_email, vendor_id, auth_email_address, auth_password, sender_name from vendor_senderid_email where is_active = true"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "InitiateEmailVendorSenderId",
			"Failed to read table email vendor sender ID. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var emailVendorSenderId sql.NullString
			var emailVendorId sql.NullString
			var emailAuthFromAddress sql.NullString
			var emailAuthPassword sql.NullString
			var emailSenderName sql.NullString

			err = rows.Scan(&emailVendorSenderId, &emailVendorId, &emailAuthFromAddress, &emailAuthPassword, &emailSenderName)

			var mapEmailVendorSenderId = make(map[string]interface{})

			mapEmailVendorSenderId["vendorSenderId"] = ConvertSQLNullStringToString(emailVendorSenderId)
			mapEmailVendorSenderId["vendorId"] = ConvertSQLNullStringToString(emailVendorId)
			mapEmailVendorSenderId["authFromAddress"] = ConvertSQLNullStringToString(emailAuthFromAddress)
			mapEmailVendorSenderId["authPassword"] = ConvertSQLNullStringToString(emailAuthPassword)
			mapEmailVendorSenderId["senderName"] = ConvertSQLNullStringToString(emailSenderName)

			mapReturn.Set(ConvertSQLNullStringToString(emailVendorSenderId), mapEmailVendorSenderId)
		}
	}

	return mapReturn
}

func IsClientSenderIdValid20(hashMapClientSenderId *hashmap.HashMap, messageId string, clientId string, clientSenderId string) (bool, string) {
	isValid := false
	clientSenderIdId := ""

	clientSenderIdId = strings.TrimSpace(clientSenderId) + "-" + strings.TrimSpace(clientId)

	mapDetailClientSenderId, ok := hashMapClientSenderId.Get(clientSenderIdId)

	if !ok {
		isValid = false
	} else {
		if len(mapDetailClientSenderId.(map[string]string)) > 0 {
			isValid = true
		} else {
			isValid = false
		}
	}

	DoLog("INFO", messageId, "BlastMeFunctions", "IsClientSenderIdValid20",
		"clientId: "+clientId+", clientSenderId: "+clientSenderId+" -> clientSenderIdId: "+
			clientSenderIdId+", isValid: "+fmt.Sprintf("%t", isValid), false, nil)

	return isValid, clientSenderIdId
}

func IsClientSenderIdValidRedis(messageId string, redisClient *redis.Client, goContext context.Context, clientId string, clientSenderId string) (bool, string) {
	isValid := false
	clientSenderIdId := strings.TrimSpace(clientSenderId) + "-" + strings.TrimSpace(clientId)

	mapClientSenderId := GetClientSenderIdRedis(messageId, redisClient, goContext, clientId, clientSenderId)

	if len(mapClientSenderId) > 0 {
		isValid = true
	}

	return isValid, clientSenderIdId
}

//noinspection GoUnusedExportedFunction
func SMSFunctionGetClientBalance(messageId string, db *sql.DB, clientId string) float64 {
	var balance = 0.00

	query := "select now_balance from client_balance where client_id = '" + clientId + "' and expiry_date >= '" +
		DoFormatDateTime("YYYY-0M-0D HH:mm:ss", time.Now()) + "'"
	fmt.Println("query: " + query)

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "SMSFUNCTIONS", "GetClientBalance",
			"Failed to read database Webclient Balance. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&balance)
		}

		DoLog("INFO", "", "SMSFUNCTIONS", "GetClientBalance",
			"Webclient: "+clientId+" -> Balance: "+fmt.Sprintf("%.5f", balance), false, nil)
	}
	DoLog("INFO", messageId, "SMSFUNCTIONS", "GetClientBalance",
		"Webclient: "+clientId+" -> Balance: "+fmt.Sprintf("%.5f", balance), false, nil)

	return balance
}

//noinspection GoUnusedExportedFunction
func SMSFunctionIsGSM(message string) bool {
	const gsmCharacters = "@£$¥èéùìòÇØøÅåΔ_ΦΓΛΩΠΨΣΘΞ^{}\\[~]|€ÆæßÉ!\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà\n "
	isGsm := false

	runes := []rune(message)
	for x := 0; x < len(runes); x++ {
		theChar := runes[x : x+1]
		fmt.Println(fmt.Sprintf("%d. theChar: %s\n", x, string(theChar)))

		if strings.Contains(gsmCharacters, string(theChar)) {
			isGsm = true
		} else {
			isGsm = false
			break
		}
	}

	return isGsm
}

func SMSFunctionGetMessageCountAndLength(messageId string, message string, encoding string) (int32, int32) {
	var msgLength int32 = 0
	var msgCount int32 = 1
	fmt.Println("message in SMSFunctionGetMessageCountAndLength: ", message)

	// Count message length
	runes := []rune(message)
	if strings.ToLower(encoding) == "whatsapp" {
		// Whatsapp message always counted as 1 message, disregard the message length.
		msgLength = int32(len(message))
		msgCount = 1
	} else if strings.ToLower(encoding) == "ucs" || strings.ToLower(encoding) == "ucs2" {
		// UCS2
		for x := 0; x < len(runes); x++ {
			//theChar := string(runes[x:x+1])
			//fmt.Println(fmt.Sprintf("%d. theChar: %s\n", x, theChar))

			msgLength = msgLength + 1
		}
	} else {
		// GSM
		for x := 0; x < len(runes); x++ {
			theChar := string(runes[x : x+1])
			//fmt.Println(fmt.Sprintf("%d. theChar: %s\n", x, theChar))

			if theChar == "^" || theChar == "{" || theChar == "}" || theChar == "\\" || theChar == "[" || theChar == "~" || theChar == "]" || theChar == "|" || theChar == "€" {
				msgLength = msgLength + 2
			} else {
				msgLength = msgLength + 1
			}
		}
	}

	fmt.Println(fmt.Sprintf("msgLength: %d", msgLength))

	// Count msgCount
	if strings.ToLower(encoding) == "ucs" || strings.ToLower(encoding) == "ucs2" {
		// UCS2
		if msgLength > 70 {
			msgCount = int32(math.Ceil(float64(msgLength) / float64(66)))
		} else {
			msgCount = 1
		}
	} else if strings.ToLower(encoding) == "voice" {
		// VOICE is per 100
		if msgLength > 100 {
			msgCount = int32(math.Ceil(float64(msgLength) / float64(100)))
		} else {
			msgCount = 1
		}
	} else if strings.ToLower(encoding) == "whatsapp" {
		// Whatsapp message always counted as 1 message, disregard the message length.
		if msgLength > 700 {
			msgCount = int32(math.Ceil(float64(msgLength) / float64(700)))
		} else {
			msgCount = 1
		}
	} else {
		// GSM
		if msgLength > 160 {
			msgCount = int32(math.Ceil(float64(msgLength) / float64(152)))
		} else {
			msgCount = 1
		}
	}

	DoLog("INFO", messageId, "SMSFUNCTIONS", "GetClientDetail",
		"msgCount: "+strconv.Itoa(int(msgCount))+", msgLength: "+strconv.Itoa(int(msgLength)), false, nil)

	return msgCount, msgLength
}

//func SmsFunctionGetRoutingTable(messageId string, routingId string) map[string]interface{} {
//	var mapRouting = make(map[string]interface{})
//
//	var redisKey = "routing-" + routingId
//	var redisVal = RedisGetDataRedis(messageId, redisKey)
//
//	if len(strings.TrimSpace(redisVal)) > 0 {
//		mapRouting = ConvertJSONStringToMap(messageId, redisVal)
//	} else {
//		mapRouting = nil
//	}
//
//	return mapRouting
//}

//func SMSFunctionGetVendorQueueName(messageId string, vendorId string) string {
//	queueVendor := ""
//
//	redisKey := "vendor-" + strings.TrimSpace(vendorId)
//	redisVal := RedisGetDataRedis(messageId, redisKey)
//
//	// Convert to map
//	if len(strings.TrimSpace(redisVal)) > 0 {
//		mapVendor := ConvertJSONStringToMap(messageId, redisVal)
//
//		queueVendor = GetStringFromMapInterface(mapVendor, "queueName")
//	}
//
//	return queueVendor
//}

//func SMSFunctionGetVendorSenderId(messageId string, vendorSenderIdId string) string {
//	vendorSenderId := ""
//
//	redisKey := "vendorSenderId-" + strings.TrimSpace(vendorSenderIdId)
//	redisVal := RedisGetDataRedis(messageId, redisKey)
//
//	// Convert to map
//	if len(strings.TrimSpace(redisVal)) > 0 {
//		mapSenderId := ConvertJSONStringToMap(messageId, redisVal)
//
//		vendorSenderId = GetStringFromMapInterface(mapSenderId, "vendorSenderId")
//	}
//
//	return vendorSenderId
//}

//func SMSFunctionIsClientSenderIdValid(messageId string, clientId string, clientSenderId string) bool {
//	isValid := false
//
//	clientSenderIdId := clientSenderId + "-" + clientId
//
//	var redisKey = "clientSenderId-" + clientSenderIdId
//	var redisVal = RedisGetDataRedis(messageId, redisKey)
//
//	if len(redisVal) > 0 {
//		mapClientSenderId := ConvertJSONStringToMap(messageId, redisVal)
//
//		if MapInterfaceHasKey(mapClientSenderId, "clientSenderIdId") {
//			isValid = true
//		} else {
//			isValid = false
//		}
//	} else {
//		isValid = false
//	}
//
//	return isValid
//}

//func SMSFunctionIsRoutingValid(messageId string, clientSenderId string, clientId string, apiUserName string, telecomId string) bool {
//	isValid := false
//
//	redisKey := "routing-" + clientSenderId + "-" + clientId + "-" + apiUserName + "-" + telecomId
//	redisVal := RedisGetDataRedis(messageId, redisKey)
//
//	if len(redisVal) > 0 {
//		mapRouting := ConvertJSONStringToMap(messageId, redisVal)
//
//		if MapInterfaceHasKey(mapRouting, "routingId") {
//			isValid = true
//		} else {
//			isValid = false
//		}
//	} else {
//		isValid = false
//	}
//
//	return isValid
//}

func InitiateRandomVendorSenderIdId(db *sql.DB) {
	query := "select random_id, vendor_sender_id_id, vendor_sender_id, message_template, vendor_id, telecom_id, vendor_parameter from vendor_senderid_random"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdId",
			"Failed to read database Random Vendor. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var randomId string
			var vendorSenderIdId string
			var vendorSenderId string
			var messageTemplate string
			var vendorId string
			var telecomId string
			var vendorParameter sql.NullString

			errScan := rows.Scan(&randomId, &vendorSenderIdId, &vendorSenderId, &messageTemplate, &vendorId, &telecomId, &vendorParameter)

			if errScan != nil {
				// Error scan
				DoLog("INFO", "", "SMSFUNCTIONS", "InitiateRandomVendorSenderIdId",
					"Failed to scan database Random Vendor. Error occured.", true, err)
			} else {
				// Success scan
				var mapRandomVendorSenderIdId = make(map[string]string)

				mapRandomVendorSenderIdId["randomId"] = randomId
				mapRandomVendorSenderIdId["vendorSenderIdId"] = vendorSenderIdId
				mapRandomVendorSenderIdId["vendorSenderId"] = vendorSenderId
				mapRandomVendorSenderIdId["vendorId"] = vendorId
				mapRandomVendorSenderIdId["messageTemplate"] = messageTemplate
				mapRandomVendorSenderIdId["vendorId"] = vendorId
				mapRandomVendorSenderIdId["telecomId"] = telecomId
				mapRandomVendorSenderIdId["vendorParameter"] = ConvertSQLNullStringToString(vendorParameter)

				// Put into redis
				redisKey := "randomVendorSenderIdMessage-" + randomId
				redisVal := ConvertMapStringToJSON(mapRandomVendorSenderIdId)

				RedisSetDataRedis("", redisKey, redisVal)
				DoLog("INFO", "", "SMSFUNCTIONS", "InitiateRandomVendorSenderIdId",
					"Initiating randomVendorSenderId to redisKey: "+redisKey+", redisVal: "+redisVal, false, nil)
			}
		}
	}
}

func SMSFunctionGetRandomVendorSenderIdMessage(messageId string, telecomId string, vendorId string) map[string]interface{} {
	var mapRandomVendorSenderId = make(map[string]interface{})

	// Get all keys pattern randomVendorSenderIdMessage-[telecomId]-[vendorId]-*
	redisKeyPattern := "randomVendorSenderIdMessage-" + telecomId + "-" + vendorId + "-*"
	DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdMessage",
		"redisKeyPattern: "+redisKeyPattern, false, nil)

	arrRedisKeys := RedisGetKeysWithPatternRedis(messageId, redisKeyPattern)
	lenRedisKeys := len(arrRedisKeys)
	DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdMessage",
		fmt.Sprintf("arrRedisKeys: %+v, lenRedisKeys: %d", arrRedisKeys, lenRedisKeys), false, nil)

	if lenRedisKeys > 0 {
		// Pick one random
		min := 0
		max := lenRedisKeys - 1
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(max-min+1) + min
		DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdMessage",
			fmt.Sprintf("seletedIndex: %d", selectedIndex), false, nil)

		selectedRandomId := arrRedisKeys[selectedIndex]
		DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdMessage",
			"selectedRandomId: "+selectedRandomId, false, nil)

		// get from redis using selectedRandomId
		redisKey := selectedRandomId
		redisVal := RedisGetDataRedis(messageId, redisKey)
		DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdMessage",
			fmt.Sprintf("Finally, redisKey: %s, redisVal: %s", redisKey, redisVal), false, nil)

		mapRandomVendorSenderId = ConvertJSONStringToMap(messageId, redisVal)
	}

	DoLog("INFO", "", "SMSFUNCTIONS", "GetRandomVendorSenderIdMessage",
		fmt.Sprintf("mapRandomVendorSenderId: %+v", mapRandomVendorSenderId), false, nil)
	return mapRandomVendorSenderId
}

func GetANumberByCountryCode(messageId string, countryCode string) string {
	countryCode = strings.TrimSpace(countryCode)
	theANumber := ""

	if countryCode == "62" { // Indonesia
		upperBound := len(NDCIndonesia)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedIndPrefix := "811"
		if selectedIndex < upperBound {
			selectedIndPrefix = NDCIndonesia[selectedIndex]
		}

		theANumber = countryCode + selectedIndPrefix + strings.TrimSpace(GenerateRandomNumericString(8))
	} else if countryCode == "852" { // Hongkong
		upperBound := len(NDCHongkong)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedHKPrefix := "677"
		if selectedIndex < upperBound {
			selectedHKPrefix = NDCHongkong[selectedIndex]
		}

		theANumber = countryCode + selectedHKPrefix + strings.TrimSpace(GenerateRandomNumericString(4))
	} else if countryCode == "60" { // Malaysia
		upperBound := len(NDCMalaysia)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedMalaysiaPrefix := "677"
		if selectedIndex < upperBound {
			selectedMalaysiaPrefix = NDCMalaysia[selectedIndex]
		}

		theANumber = countryCode + selectedMalaysiaPrefix + strings.TrimSpace(GenerateRandomNumericString(6))
	} else if countryCode == "63" { // Philippines
		upperBound := len(NDCPhilippines)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedPhilippinesPrefix := "919"
		if selectedIndex < upperBound {
			selectedPhilippinesPrefix = NDCPhilippines[selectedIndex]
		}

		theANumber = countryCode + selectedPhilippinesPrefix + strings.TrimSpace(GenerateRandomNumericString(7))
	} else if countryCode == "65" { // Singapore
		upperBound := len(NDCSingapore)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedSingPrefix := "831"
		if selectedIndex < upperBound {
			selectedSingPrefix = NDCSingapore[selectedIndex]
		}

		theANumber = countryCode + selectedSingPrefix + strings.TrimSpace(GenerateRandomNumericString(5))
	} else if countryCode == "856" { // Laos
		upperBound := len(NDCLaos)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedLaosPrefix := "302"
		if selectedIndex < upperBound {
			selectedLaosPrefix = NDCLaos[selectedIndex]
		}

		// Hitung panjang sisa
		maxLength := 12
		if strings.HasPrefix(selectedLaosPrefix, "205") || strings.HasPrefix(selectedLaosPrefix, "202") {
			maxLength = 13
		}

		sisaLength := maxLength - len(countryCode) + len(selectedLaosPrefix)

		theANumber = countryCode + selectedLaosPrefix + strings.TrimSpace(GenerateRandomNumericString(sisaLength))
	} else {
		// Default is INDONESIA (62)
		upperBound := len(NDCIndonesia)

		// Generate random index
		rand.Seed(time.Now().UnixNano())
		selectedIndex := rand.Intn(upperBound)

		selectedIndPrefix := "811"
		if selectedIndex < upperBound {
			selectedIndPrefix = NDCIndonesia[selectedIndex]
		}

		theANumber = countryCode + selectedIndPrefix + strings.TrimSpace(GenerateRandomNumericString(8))
	}

	DoLog("INFO", messageId, "BlastMeFunctions", "getANumberByCountryCode",
		"countryCode: "+countryCode+" -> aNumber: "+theANumber, false, nil)

	return theANumber
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

var floatType = reflect.TypeOf(float64(0))
var stringType = reflect.TypeOf("")

func CastInterfaceToFloat64(unk interface{}) (float64, error) {
	switch i := unk.(type) {
	case float64:
		return i, nil
	case float32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case string:
		return strconv.ParseFloat(i, 64)
	default:
		v := reflect.ValueOf(unk)
		v = reflect.Indirect(v)
		if v.Type().ConvertibleTo(floatType) {
			fv := v.Convert(floatType)
			return fv.Float(), nil
		} else if v.Type().ConvertibleTo(stringType) {
			sv := v.Convert(stringType)
			s := sv.String()
			return strconv.ParseFloat(s, 64)
		} else {
			return math.NaN(), fmt.Errorf("Can't convert %v to float64", v.Type())
		}
	}
}

func RuneToAscii(theStr string) string {
	theRune := []rune(theStr)

	finalString := ""
	for _, val := range theRune {
		if val < 128 {
			finalString = finalString + string(val)
		} else {
			finalString = finalString + fmt.Sprintf("\\u%08x", val)
		}
	}
	return finalString
}

func CheckBCryptHashPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func LoadBlackListMessageToRedis(db *sql.DB, redisClient *redis.Client, goContext context.Context) []string {
	var arrBlackListedKeywords []string

	// Query table message_blacklist
	query := "select keyword from message_blacklist where is_active = true order by keyword"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "LoadBlackListMessageToRedis",
			"Failed to read database blacklisted messages. Error occur.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var keyword sql.NullString

			err = rows.Scan(&keyword)

			if err != nil {
				// Error scan table countryCodePrefix
				DoLog("INFO", "", "BlastMeFunctions", "LoadBlackListMessageToRedis",
					"Failed to scan table blacklisted message. Error occur.", true, err)
			} else {
				arrBlackListedKeywords = append(arrBlackListedKeywords, ConvertSQLNullStringToString(keyword))
			}
		}
	}

	return arrBlackListedKeywords
}

func LoadBlackListMessageToArray(db *sql.DB) []string {
	var arrBlackListedKeywords []string

	// Query table message_blacklist
	query := "select keyword from message_blacklist where is_active = true order by keyword"

	rows, err := db.Query(query)
	if err != nil {
		DoLog("INFO", "", "BlastMeFunctions", "LoadBlackListMessageToRedis",
			"Failed to read database blacklisted messages. Error occur.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var keyword sql.NullString

			err = rows.Scan(&keyword)

			if err != nil {
				// Error scan table countryCodePrefix
				DoLog("INFO", "", "BlastMeFunctions", "LoadBlackListMessageToRedis",
					"Failed to scan table blacklisted message. Error occur.", true, err)
			} else {
				arrBlackListedKeywords = append(arrBlackListedKeywords, ConvertSQLNullStringToString(keyword))
			}
		}
	}

	return arrBlackListedKeywords
}

// Inject SMPP end-to-end DR
func SubmitEndToEndDLRtoQueue(db *sql.DB, drIncoming *amqp.Channel, queueDLR string, messageId string, callbackDateTime time.Time, vendorErrorCode string, vendorStatus string) {

	vendorCodeStatus := ""

	// Get ClientSenderID
	queryGetClientSenderID := "SELECT msisdn, client_sender_id FROM transaction_sms WHERE message_id = $1"

	rows, err := db.Query(queryGetClientSenderID, messageId)

	if err != nil {
		DoLog("INFO", messageId, "REALDR", "SubmitRealDRtoQueue",
			"Failed to read sms transaction. Error occured.", true, err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var rawMSISDN sql.NullString
			var rawClientSenderID sql.NullString

			errScan := rows.Scan(&rawMSISDN, &rawClientSenderID)

			if errScan != nil {
				// Failed to scan
				DoLog("INFO", "", "REALDR", "SubmitRealDRtoQueue",
					"Failed to scan client balance. Error occured.", true, errScan)
			} else {
				// queueDLR:
				// {
				//		"messageId":"a81ecf1f591741cdb94c74ad73a71577",
				//		"msisdn":"8618092070912",
				//		"status":"000",
				//		"errorCode":"000",
				//		"clientSenderId":"AUTOGEN",
				//		"dateTime":"2022-09-06 23:08:40.755"
				//	}

				strMSISDN := ConvertSQLNullStringToString(rawMSISDN)
				strClientSenderID := ConvertSQLNullStringToString(rawClientSenderID)

				// DELIVRD --> 000
				// REJECTD --> 001
				// ACCEPTD --> 002
				// UNDELIV --> 221
				// EXPIRED --> 222

				if vendorStatus == "DELIVRD" {
					vendorCodeStatus = "000"
				} else if vendorStatus == "ACCEPTD" {
					vendorCodeStatus = "002"
				} else if vendorStatus == "EXPIRED" {
					vendorCodeStatus = "222"
				} else {
					vendorCodeStatus = "221"
				}

				var mapDLR = make(map[string]interface{})
				mapDLR["messageId"] = messageId
				mapDLR["msisdn"] = strMSISDN
				mapDLR["status"] = vendorCodeStatus
				mapDLR["errorCode"] = vendorErrorCode
				mapDLR["clientSenderId"] = strClientSenderID
				mapDLR["dateTime"] = callbackDateTime

				// Convert to JSON
				jsonDLR := ConvertMapInterfaceToJSON(mapDLR)

				fmt.Println("Json DLR : ", jsonDLR)

				errP := drIncoming.Publish(
					"",       // exchange
					queueDLR, // routing key
					false,    // mandatory
					false,
					amqp.Publishing{
						DeliveryMode: amqp.Persistent,
						ContentType:  "text/plain",
						Body:         []byte(jsonDLR),
					})

				if errP != nil {
					DoLog("DEBUG", messageId, "REALDR", "doSubmitToRouter",
						"Failed to submit to queue: "+queueDLR+". Error occured. Trying to re-initiate the channel.", true, errP)

				} else {
					DoLog("DEBUG", messageId, "REALDR", "doSubmitToRouter",
						"Successfully to publish to SMS INCOMING Queue "+queueDLR, false, nil)
				}
			}
		}
	}
}
