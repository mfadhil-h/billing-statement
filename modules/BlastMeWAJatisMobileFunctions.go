package modules

import (
	"database/sql"
	"fmt"
	"github.com/cornelk/hashmap"
	_ "github.com/gabriel-vasile/mimetype"
	"strings"
)

// WA Jatis Mobile Account
// var userId = "jatismobiletesting"
// var password = "JatisMbl9048$"
var jmUrl = "https://interactive.jatismobile.com"

//func InitiateWABAJamobProperty(db *sql.DB) {
//	query := "select client_id, user_id, password, call_url_path, handler_name from wa_jamob_property"
//
//	rows, err := db.Query(query)
//
//	if err != nil {
//		panic(err)
//	} else {
//		defer rows.Close()
//
//		// Delete old records
//		RedisDelKeysWithPatternRedis("", "wabaJamob20-*")
//
//		for rows.Next() {
//			var clientId string
//			var userId string
//			var password string
//			var callUrlPath string
//			var handlerName string
//
//			err := rows.Scan(&clientId, &userId, &password, &callUrlPath, &handlerName)
//
//			if err != nil {
//				DoLog("DEBUG", "", "WAJamobFunctions", "LoadWABAJamobProperty",
//					"Failed to scan table waba jamob property.", true, err)
//			} else {
//				// Get detai by clientId
//				mapWabaProperty := make(map[string]string)
//
//				mapWabaProperty["clientId"] = clientId
//				mapWabaProperty["userId"] = userId
//				mapWabaProperty["password"] = password
//				mapWabaProperty["urlPath"] = callUrlPath
//				mapWabaProperty["handlerName"] = handlerName
//
//				// Submit to redis
//				redisKey := "wabaJamob20-" + clientId
//				redisVal := ConvertMapStringToJSON(mapWabaProperty)
//
//				RedisSetDataRedis("", redisKey, redisVal)
//
//				// Get detail by url path
//				mapHandler := make(map[string]string)
//
//				mapHandler["clientId"] = clientId
//				mapHandler["userId"] = userId
//				mapHandler["password"] = password
//				mapHandler["handlerName"] = handlerName
//
//				// Submit to redis
//				redisKeyHandler := "wainteractivemapping-" + callUrlPath
//				redisValHandler := ConvertMapStringToJSON(mapHandler)
//
//				RedisSetDataRedis("", redisKeyHandler, redisValHandler)
//			}
//		}
//	}
//}

func InitiateWABAJamobProperty20(db *sql.DB) (*hashmap.HashMap, *hashmap.HashMap) {
	var mapWABAByClientId = &hashmap.HashMap{}
	var mapWABAByURL = &hashmap.HashMap{}

	query := "select client_id, user_id, password, call_url_path, handler_name from wa_jamob_property"

	rows, err := db.Query(query)

	if err != nil {
		panic(err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var clientId sql.NullString
			var userId sql.NullString
			var password sql.NullString
			var callUrlPath sql.NullString
			var handlerName sql.NullString

			err := rows.Scan(&clientId, &userId, &password, &callUrlPath, &handlerName)

			if err != nil {
				DoLog("DEBUG", "", "WAJamobFunctions", "LoadWABAJamobProperty",
					"Failed to scan table waba jamob property.", true, err)
			} else {
				// Get detai by clientId
				mapWabaProperty := make(map[string]string)

				mapWabaProperty["clientId"] = ConvertSQLNullStringToString(clientId)
				mapWabaProperty["userId"] = ConvertSQLNullStringToString(userId)
				mapWabaProperty["password"] = ConvertSQLNullStringToString(password)
				mapWabaProperty["urlPath"] = ConvertSQLNullStringToString(callUrlPath)
				mapWabaProperty["handlerName"] = ConvertSQLNullStringToString(handlerName)

				// Put into mapWABAByClientId
				mapWABAByClientId.Set(ConvertSQLNullStringToString(clientId), mapWabaProperty)

				// Get detail by url path
				mapHandler := make(map[string]string)

				mapHandler["clientId"] = ConvertSQLNullStringToString(clientId)
				mapHandler["userId"] = ConvertSQLNullStringToString(userId)
				mapHandler["password"] = ConvertSQLNullStringToString(password)
				mapHandler["handlerName"] = ConvertSQLNullStringToString(handlerName)

				// Put into mapWABAByURL
				mapWABAByURL.Set(ConvertSQLNullStringToString(callUrlPath), mapHandler)
			}
		}
	}

	return mapWABAByClientId, mapWABAByURL
}

func DoLoginWAJatisMobile20(clientId string, jmUserId string, jmPassword string) string {
	DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
		"Logging In for client Id: "+clientId+" to WABA JAMOB.", false, nil)

	loginStatus := "000"

	var urlLogin = jmUrl + "/wa/users/login"
	fmt.Println("Login URL: " + urlLogin)

	// Authorization
	authorization := EncodeBase64(jmUserId + ":" + jmPassword)
	fmt.Println("base64 Authorization: " + authorization)

	// Prepare the header
	var mapHeader = make(map[string]interface{})
	mapHeader["Authorization"] = "Basic " + authorization
	mapHeader["Content-Type"] = "application/json"

	// Prepare the body
	theBody := "{}" // Just empty

	// Hit Jatis Mobile
	mapHit := HTTPSPOSTString("", urlLogin, mapHeader, theBody)
	DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
		"mapHit: "+fmt.Sprintf("%+v", mapHit), false, nil)

	// Handle mapHit
	httpStatus := GetStringFromMapInterface(mapHit, "httpStatus")
	opStatus := GetStringFromMapInterface(mapHit, "status")
	bodyResponse := GetStringFromMapInterface(mapHit, "bodyResponse")
	DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
		"bodyResponse: "+bodyResponse, false, nil)

	if httpStatus == "200" && opStatus == "000" {
		// Hit sukses check body result
		// Convert bodyResult to map[string]interface
		mapBodyResult := ConvertJSONStringToMap("", bodyResponse)

		if MapInterfaceHasKey(mapBodyResult, "errors") {
			mapError := mapBodyResult["errors"].([]interface{})[0].(map[string]interface{})
			DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
				"mapError[0]: "+fmt.Sprintf("%+v", mapError), false, nil)

			if MapInterfaceHasKey(mapError, "code") {
				errCode := mapError["code"].(float64)

				if errCode == 0 {
					// Success Login
					// - Get the token with expiry
					// - The login process will be commenced everyday, so ignoring the expiry
					if MapInterfaceHasKey(mapBodyResult, "users") {
						mapUsers := mapBodyResult["users"].([]interface{})[0].(map[string]interface{})
						DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
							"mapUsers[0]: "+fmt.Sprintf("%+v", mapUsers), false, nil)

						loginToken := GetStringFromMapInterface(mapUsers, "token")
						tokenExpiry := GetStringFromMapInterface(mapUsers, "expires_after")
						DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
							"loginToken: "+loginToken+", tokenExpiry: "+tokenExpiry, false, nil)

						// Store in redis
						redisKey := "wainteractivejatismobileToken-" + jmUserId

						var mapRedis = make(map[string]string)
						mapRedis["token"] = loginToken
						mapRedis["expiry"] = tokenExpiry

						redisVal := ConvertMapStringToJSON(mapRedis)
						RedisSetDataRedis("", redisKey, redisVal)
						DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
							"Token is stored to redis - redisKey: "+redisKey+", redisVal: "+redisVal, false, nil)

						loginStatus = "000"
					} else {
						// Jadi gimana nih? Error atau success?
						// Anggap error deh, jadi exit
						DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
							"FAILED TO LOGIN TO JATIS MOBILE WA INTERACTIVE SERVER, I DON'T KNOW WHY!", false, nil)

						loginStatus = "201"
					}
				} else if errCode == 404 {
					// Wrong Username and or Password
					DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
						"FAILED TO LOGIN TO JATIS MOBILE WA INTERACTIVE SERVER, INVALID USERNAME/PASSWORD!", false, nil)

					loginStatus = "204"
				} else {
					// Wrong Username and or Password
					DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
						"FAILED TO LOGIN TO JATIS MOBILE WA INTERACTIVE SERVER.", false, nil)

					loginStatus = "204"
				}
			} else {
				// Login Gak jelas
				// Assumed login success
				// - Get the token with expiry
				// - The login process will be commenced everyday, so ignoring the expiry
				if MapInterfaceHasKey(mapBodyResult, "users") {
					mapUsers := mapBodyResult["users"].([]interface{})[0].(map[string]interface{})
					DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
						"mapUsers[0]: "+fmt.Sprintf("%+v", mapUsers), false, nil)

					loginToken := GetStringFromMapInterface(mapUsers, "token")
					tokenExpiry := GetStringFromMapInterface(mapUsers, "expires_after")
					DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
						"loginToken: "+loginToken+", tokenExpiry: "+tokenExpiry, false, nil)

					// Store in redis
					redisKey := "wainteractivejatismobileToken-" + jmUserId

					var mapRedis = make(map[string]string)
					mapRedis["token"] = loginToken
					mapRedis["expiry"] = tokenExpiry

					redisVal := ConvertMapStringToJSON(mapRedis)
					RedisSetDataRedis("", redisKey, redisVal)
					DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
						"Token is stored to redis - redisKey: "+redisKey+", redisVal: "+redisVal, false, nil)

					loginStatus = "000"
				} else {
					// Jadi gimana nih? Error atau success?
					// Anggap error deh, jadi exit
					DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
						"FAILED TO LOGIN TO JATIS MOBILE WA INTERACTIVE SERVER, I DON'T KNOW WHY! PUSING ANE GAN!", false, nil)

					loginStatus = "240"
				}
			}
		} else {
			// Assumed login success
			// - Get the token with expiry
			// - The login process will be commenced everyday, so ignoring the expiry
			if MapInterfaceHasKey(mapBodyResult, "users") {
				mapUsers := mapBodyResult["users"].([]interface{})[0].(map[string]interface{})
				DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
					"mapUsers[0]: "+fmt.Sprintf("%+v", mapUsers), false, nil)

				loginToken := GetStringFromMapInterface(mapUsers, "token")
				tokenExpiry := GetStringFromMapInterface(mapUsers, "expires_after")
				DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
					"loginToken: "+loginToken+", tokenExpiry: "+tokenExpiry, false, nil)

				// Store in redis
				redisKey := "wainteractivejatismobileToken-" + jmUserId

				var mapRedis = make(map[string]string)
				mapRedis["token"] = loginToken
				mapRedis["expiry"] = tokenExpiry

				redisVal := ConvertMapStringToJSON(mapRedis)
				RedisSetDataRedis("", redisKey, redisVal)
				DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
					"Token is stored to redis - redisKey: "+redisKey+", redisVal: "+redisVal, false, nil)

				loginStatus = "000"
			} else {
				// Jadi gimana nih? Error atau success?
				// Anggap error deh, jadi exit
				DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
					"FAILED TO LOGIN TO JATIS MOBILE WA INTERACTIVE SERVER, I DON'T KNOW WHY! PUSING ANE GAN!", false, nil)

				loginStatus = "240"
			}
		}
	} else {
		// Hit failed - no need to check the body result
		fmt.Println("\n\n\n*** FAILED TO HIT JATIS MOBILE API SERVERS! ***")
		DoLog("DEBUG", "", "WAInteractiveJatisMobileAgent", "doLoginWAJatisMobile",
			"FAILED TO HIT JATIS MOBILE API SERVERS!", false, nil)

		loginStatus = "240"
	}

	return loginStatus
}

func GetJatisMobileWAToken(messageId string, userId string) (string, string) {
	token := ""
	expiry := ""

	redisKey := "wainteractivejatismobileToken-" + userId
	redisVal := RedisGetDataRedis(messageId, redisKey)
	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "GetJatisMobileWAToken",
		"redisKey: "+redisKey+" -> redisVal: "+redisVal, false, nil)

	// Convert redisVal to token and expiry
	if len(strings.TrimSpace(redisVal)) > 0 {
		mapRedis := ConvertJSONStringToMap(messageId, redisVal)

		token = GetStringFromMapInterface(mapRedis, "token")
		expiry = GetStringFromMapInterface(mapRedis, "expiry")
	} else {
		// Not found in redis
	}
	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "GetJatisMobileWAToken",
		"token: "+token+", expiry: "+expiry, false, nil)

	return token, expiry
}

func DoSettingWebHookPerClient20(messageId string, mapWABAProperty *hashmap.HashMap, clientId string, webHookUrl string) string {
	theReturn := ""

	userId := GetWAUserIdByClientId20(messageId, mapWABAProperty, clientId)
	theReturn = DoSettingWebHook20(messageId, userId, webHookUrl)

	return theReturn
}

func DoSettingWebHook20(messageId string, waJMUserId string, webHookUrl string) string {
	settingStatus := "000"

	var urlSettingWebHook = jmUrl + "/wa/settings/application"
	fmt.Println("Setting Web Hook URL: " + urlSettingWebHook)

	// Get token
	token, expiry := GetJatisMobileWAToken(messageId, waJMUserId)
	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSettingWebHook",
		"Getting token and expiry - token: "+token+", expiry: "+expiry, false, nil)

	if len(strings.TrimSpace(token)) > 0 {
		// token is available

		// Hit header
		var mapHeader = make(map[string]interface{})
		mapHeader["Authorization"] = "Bearer " + token

		// Hit content - jsonWebHooks
		jsonWebHooks := `{"webhooks": {"url": "` + webHookUrl + `"}}`

		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSettingWebHook",
			"Hit Parameter - mapHeader: "+fmt.Sprintf("%+v", mapHeader)+", content: "+jsonWebHooks, false, nil)

		// Hit Jatis WA API
		mapHit := HTTPSPatchString(messageId, urlSettingWebHook, mapHeader, jsonWebHooks)
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSettingWebHook",
			"mapHit: "+fmt.Sprintf("%+v", mapHit), false, nil)

		settingStatus = "000"
	} else {
		// token is not available
		settingStatus = "240"
	}

	return settingStatus
}

func DoSendingWAInteractiveMessage(messageId string, waJMUserId string, content string) string {
	// content HAS TO COMPLY WITH JATISMOBILE WA INTERACTIVE FORMAT
	sendingStatus := "000"
	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingMessage",
		"doSendingMessage - waJMUserId: "+waJMUserId+", content: "+content, false, nil)

	var urlSendingMessage = jmUrl + "/wa/messages"
	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingMessage",
		"urlSendingMessage: "+urlSendingMessage, false, nil)

	// Get token
	token, expiry := GetJatisMobileWAToken(messageId, waJMUserId)
	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingMessage",
		"Getting token and expiry - token: "+token+", expiry: "+expiry, false, nil)

	if len(strings.TrimSpace(token)) > 0 {
		// token is available

		// Hit header
		var mapHeader = make(map[string]interface{})
		mapHeader["Authorization"] = "Bearer " + token
		mapHeader["Content-Type"] = "application/json"

		// Hit Jatis WA API
		mapHit := HTTPSPOSTString(messageId, urlSendingMessage, mapHeader, content)
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSettingWebHook",
			"mapHit: "+fmt.Sprintf("%+v", mapHit), false, nil)

		sendingStatus = "000"
	} else {
		// token is not available
		sendingStatus = "240"
	}

	return sendingStatus
}

//goland:noinspection GoUnusedParameter
func GetWAUserIdByClientId20(messageId string, mapWABAByClientId *hashmap.HashMap, clientId string) string {
	theReturn := ""

	// Get waba property by clientId
	rawMapWABAProperty, ok := mapWABAByClientId.Get(clientId)

	if !ok {
		// Failed to get mapWABA
		DoLog("INFO", "", "WAJamobFunction", "GetWAUserIdByClientId",
			"Failed to get property wa jamob from redis. Empty.", false, nil)
	} else {
		mapWABAProperty := rawMapWABAProperty.(map[string]string)

		if len(mapWABAProperty) > 0 {
			theReturn = mapWABAProperty["userId"]
		} else {
			// mapRedis is empty, could be mistake happens while parsing from json string to map
			DoLog("INFO", "", "WAJamobFunction", "GetWAUserIdByClientId",
				"Failed convert json waba property from redis.", false, nil)
		}

	}

	return theReturn
}

func DoSendingWAMessageByClient20(messageId string, clientId string, mapWABAByClientId *hashmap.HashMap, content string) string {
	theReturn := ""

	userId := GetWAUserIdByClientId20(messageId, mapWABAByClientId, clientId)
	theReturn = DoSendingWAInteractiveMessage("", userId, content)

	return theReturn
}

func getFileExtByMimeType(mimeType string) string {
	fileExt := ""
	if strings.HasSuffix(mimeType, "jpeg") {
		fileExt = "jpg"
	} else {
		slashPos := strings.Index(mimeType, "/")
		fileExt = mimeType[slashPos:]
	}

	return fileExt
}

func DoDownloadWhatsAppBinary(messageId string, waJMUserId string, mediaId string, mimeType string, downloadedDirectory string) string {
	downloadStatus := "000"

	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppBinary",
		"DoDownloadWhatsAppBinary - waJMUserId: "+waJMUserId+", mediaId: "+mediaId, false, nil)

	// Initiate fileStorage
	if !strings.HasSuffix(downloadedDirectory, "/") {
		downloadedDirectory = downloadedDirectory + "/"
	}

	wabaFileStorage := downloadedDirectory
	DoLog("DEBUG", "", "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
		"WhatsApp Business file storage: "+wabaFileStorage, false, nil)

	if len(mediaId) > 0 {
		var urlDownloadMedia = jmUrl + "/wa/media/download?media_id=" + mediaId
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
			"urlDownloadMedia: "+urlDownloadMedia, false, nil)

		// Get token
		token, expiry := GetJatisMobileWAToken(messageId, waJMUserId)
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
			"Getting token and expiry - token: "+token+", expiry: "+expiry, false, nil)

		if len(strings.TrimSpace(token)) > 0 {
			// token is available

			// Get file ext by mimeType
			fileExt := getFileExtByMimeType(mimeType)
			DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
				"mimeType: "+mimeType+" -> fileExt: "+fileExt, false, nil)

			// Hit header
			var mapHeader = make(map[string]interface{})
			mapHeader["Authorization"] = "Bearer " + token

			// Hit Jatis WA API to download the file
			targetFileName := "wa_" + mediaId + "." + fileExt
			mapDownload := HTTPSGETDownloadFile(messageId, urlDownloadMedia, mapHeader, wabaFileStorage, targetFileName, 60)
			DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
				"mapDownload: "+fmt.Sprintf("%+v", mapDownload), false, nil)

			downloadStatus = "000"
		} else {
			// token is not available
			downloadStatus = "240"
		}
	} else {
		downloadStatus = "240"
	}

	return downloadStatus
}

func DoDownloadWhatsAppImage(messageId string, waJMUserId string, content string) string {
	downloadStatus := "000"

	DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
		"DoDownloadWhatsAppImage - waJMUserId: "+waJMUserId+", content: "+content, false, nil)

	// Initiate fileStorage
	wabaFileStorage := MapConfig["whatsappBusinessFileStorage"]
	DoLog("DEBUG", "", "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
		"WhatsApp Business file storage: "+wabaFileStorage, false, nil)

	// Convert content to map
	mapContent := ConvertJSONStringToMap(messageId, content)
	mediaId := GetStringFromMapInterface(mapContent, "image_id")
	mediaExt := GetStringFromMapInterface(mapContent, "file_ext")

	if len(content) > 0 && len(mediaId) > 0 {
		var urlDownloadMedia = jmUrl + "/wa/media/download?media_id=" + mediaId
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
			"urlDownloadMedia: "+urlDownloadMedia, false, nil)

		// Get token
		token, expiry := GetJatisMobileWAToken(messageId, waJMUserId)
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
			"Getting token and expiry - token: "+token+", expiry: "+expiry, false, nil)

		if len(strings.TrimSpace(token)) > 0 {
			// token is available

			// Hit header
			var mapHeader = make(map[string]interface{})
			mapHeader["Authorization"] = "Bearer " + token
			//mapHeader["Content-Type"] = "application/json"

			// Hit Jatis WA API to download the file
			targetFileName := "wa_" + mediaId + "." + mediaExt
			mapDownload := HTTPSGETDownloadFile(messageId, urlDownloadMedia, mapHeader, wabaFileStorage, targetFileName, 60)
			DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoDownloadWhatsAppImage",
				"mapDownload: "+fmt.Sprintf("%+v", mapDownload), false, nil)

			downloadStatus = "000"
		} else {
			// token is not available
			downloadStatus = "240"
		}
	} else {
		downloadStatus = "240"
	}

	return downloadStatus
}

func DoDownloadWAImageByClient20(messageId string, mapWABAProperty *hashmap.HashMap, clientId string, content string) string {
	theReturn := ""

	userId := GetWAUserIdByClientId20(messageId, mapWABAProperty, clientId)
	theReturn = DoDownloadWhatsAppImage(messageId, userId, content)

	return theReturn
}

func SendWAText20(messageId string, mapWABAProperty *hashmap.HashMap, clientId string, toPhoneNumber string, theTextMessage string) {
	// format it JSON:
	// {
	//		"recipient_type": "individual",
	//		"to": "whatsapp-id",
	//		"type": "text",
	//		"text": {
	//			"body": "your-text-message-content"
	//		}
	//	}

	jsonString := `{
			"recipient_type": "individual",
			"to": "` + toPhoneNumber + `",
			"type": "text",
			"text": {
				"body": "` + theTextMessage + `"
			}
		}`

	sendingResult := DoSendingWAMessageByClient20(messageId, clientId, mapWABAProperty, jsonString)
	DoLog("DEBUG", messageId, "WABAJamobFunctions", "sendWAText20",
		"Sending WA Text to: "+toPhoneNumber+", textMessage: "+theTextMessage+" by clientId: "+clientId+
			" -> sendingResult: "+sendingResult, false, nil)
}

func SendWABinaryContent20(messageId string, mapWABAProperty *hashmap.HashMap, clientId string, contentType string, toPhoneNumber string, theContentLink string, theCaption string) {
	// format it JSON:
	// {
	//		"recipient_type": "individual",
	//		"to": "whatsapp-id",
	//		"type": "audio" | "document" | "image" | "video",

	//		"audio": {
	//			"link": "http(s)://the-url.mp3"
	//		}

	//		"document": {
	//			"link": "http(s)://the-url.pdf",
	//			"caption": "your-document-caption"
	//		}

	//		"image": {
	//			"link": "http(s)://the-url.jpg",
	//			"caption": "your-image-caption"
	//		}

	//		"video": {
	//			"link": "http(s)://the-url.mp4",
	//			"caption": "your-video-caption"
	//		}
	// }

	jsonString := `{
		"recipient_type": "individual",
		"to": "` + toPhoneNumber + `",
		"type": "image",
		"image": {
			"link": "` + theContentLink + `",
			"caption": "` + theCaption + `"
		}
	}`

	if contentType == "image" {
		jsonString = `{
			"recipient_type": "individual",
			"to": "` + toPhoneNumber + `",
			"type": "image",
			"image": {
				"link": "` + theContentLink + `",
				"caption": "` + theCaption + `"
			}
		}`
	} else if contentType == "audio" {
		jsonString = `{
			"recipient_type": "individual",
			"to": "` + toPhoneNumber + `",
			"type": "audio",
			"audio": {
				"link": "` + theContentLink + `"
			}
		}`
	} else if contentType == "document" {
		jsonString = `{
			"recipient_type": "individual",
			"to": "` + toPhoneNumber + `",
			"type": "document",
			"document": {
				"link": "` + theContentLink + `",
				"caption": "` + theCaption + `"
			}
		}`
	} else if contentType == "video" {
		jsonString = `{
			"recipient_type": "individual",
			"to": "` + toPhoneNumber + `",
			"type": "video",
			"video": {
				"link": "` + theContentLink + `",
				"caption": "` + theCaption + `"
			}
		}`
	}

	sendingResult := DoSendingWAMessageByClient20(messageId, clientId, mapWABAProperty, jsonString)
	DoLog("DEBUG", messageId, "WABAJamobFunctions", "sendWABinaryContent20",
		"Sending WA Binary Content to: "+toPhoneNumber+", contentType: "+contentType+", contentLink:"+
			theContentLink+", theCaption: "+theCaption+" -> sendingResult: "+sendingResult, false, nil)
}

func DownloadWAImage20(messageId string, mapWABAProperty *hashmap.HashMap, clientId string, imageId string, fileExt string) {
	// format it JSON:
	// {
	//		"image_id": "1234567890"
	// }

	jsonString := `{
		"image_id": "` + imageId + `",
		"file_ext": "` + fileExt + `"
	}`

	downloadResult := DoDownloadWAImageByClient20(messageId, mapWABAProperty, clientId, jsonString)
	DoLog("DEBUG", messageId, "WABAJamobFunctions", "DownloadWAImage20",
		"Downloading wa image, imageId: "+imageId+", fileExt: "+fileExt+" -> downloadResult: "+downloadResult, false, nil)
}

func GetWAHandler20(messageId string, mapWABAPropertyByURLPath *hashmap.HashMap, path string) map[string]string {
	//redisKey := "wainteractivemapping-" + strings.TrimSpace(path)
	//redisVal := RedisGetDataRedis(messageId, redisKey)
	//
	//// Convert redisVal to map[string]string
	//mapReturn := ConvertJSONStringToMap(messageId, redisVal)

	rawMapReturn, ok := mapWABAPropertyByURLPath.Get(path)

	mapReturn := make(map[string]string)
	if !ok {
		DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "GetWAHandler20",
			"Failed to get WA Handler for URL Path: "+path, false, nil)
	} else {
		mapReturn = rawMapReturn.(map[string]string)
	}

	return mapReturn
}

func SendWAMessageTemplateByClient20(messageId string, mapWABAPropertyByClient *hashmap.HashMap, clientId string, content string) string {
	sendingStatus := "000"

	//// Get waba property by clientId
	//redisKey := "wabaJamob20-" + clientId
	//redisVal := RedisGetDataRedis("", redisKey)

	rawMapWABAProperty, ok := mapWABAPropertyByClient.Get(clientId)

	if !ok {
		DoLog("INFO", "", "WAJamobFunction", "DoSendingWAMessageTemplateByClient20",
			"Failed to get property wa jamob from redis. Empty.", false, nil)
	} else {
		mapWABAProperty := rawMapWABAProperty.(map[string]string)

		if len(mapWABAProperty) > 0 {
			userId := mapWABAProperty["userId"]

			DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingWAMessageTemplateByClient20",
				"clientId: "+clientId+" -> waUserId: "+userId, false, nil)

			var urlSendingMessage = jmUrl + "/v1/messages"
			DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingWAMessageTemplateByClient20",
				"urlSendingMessage: "+urlSendingMessage, false, nil)

			// Get token
			token, expiry := GetJatisMobileWAToken(messageId, userId)
			DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingWAMessageTemplateByClient20",
				"Getting token and expiry - token: "+token+", expiry: "+expiry, false, nil)

			if len(strings.TrimSpace(token)) > 0 {
				// token is available

				// Hit header
				var mapHeader = make(map[string]interface{})
				mapHeader["Authorization"] = "Bearer " + token
				mapHeader["Content-Type"] = "application/json"

				// Hit Jatis WA API
				//mapHit := HTTPSPOSTString(messageId, urlSendingMessage, mapHeader, content)
				mapHit := HTTPSPOSTStringFast(messageId, urlSendingMessage, mapHeader, content)
				DoLog("DEBUG", messageId, "BlastMeWAJatisMobileFunctions", "DoSendingWAMessageTemplateByClient20",
					"mapHit: "+fmt.Sprintf("%+v", mapHit), false, nil)

				// Make proper response
				hitStatus := GetStringFromMapInterface(mapHit, "status")
				httpStatus := int(mapHit["httpStatus"].(float64))

				if hitStatus == "000" && httpStatus == 200 {
					sendingStatus = "000"
				} else {
					sendingStatus = "900"
				}

			} else {
				// token is not available
				sendingStatus = "901"
			}
		} else {
			// mapRedis is empty, could be mistake happens while parsing from json string to map
			DoLog("INFO", "", "WAJamobFunction", "DoSendingWAMessageTemplateByClient20",
				"Failed to get data waba property for client "+clientId, false, nil)
		}
	}

	return sendingStatus
}
