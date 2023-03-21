package modules

import (
	"billing/Config"
	"context"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v8"
	guuid "github.com/google/uuid"
	"strings"
)

const redisToken = "authtoken_"

func toChar(i int) rune {
	return rune('A' - 1 + i)
}

func GetIPAddress(incRemoteAddress string) string {
	strIPAddress := strings.Split(incRemoteAddress, ":")
	return strIPAddress[0]
}

func GenerateGroupID(incName string) string {

	intResult := int32(0)

	incName = strings.Replace(incName, " ", "", -1)
	strPrefix := incName[:3]
	strSuffix := incName[len(incName)-3:]
	strName := strings.ToUpper(strPrefix + strSuffix)

	for _, c := range strName {
		intResult = intResult + c
	}
	strResult := fmt.Sprintf("%v", intResult)

	_, encResult := SimpleStringEncrypt(strResult, Config.ConstDefaultGroupID)
	strEnc := strings.ToUpper(encResult[:4])

	theID := strName + strEnc + strResult

	return theID
}

func GenerateClientID(incName string) string {

	intResult := int32(0)

	incName = strings.Replace(incName, " ", "", -1)
	strPrefix := incName[:3]
	strSuffix := incName[len(incName)-3:]
	strName := strings.ToUpper(strPrefix + strSuffix)

	for _, c := range strName {
		intResult = intResult + c
	}
	strResult := fmt.Sprintf("%v", intResult)

	_, encResult := SimpleStringEncrypt(strResult, Config.ConstDefaultClientID)
	strEnc := strings.ToUpper(encResult[:3])

	theID := strName + strEnc + strResult

	return theID

}

func GenerateFormulaID(incClientID string, incName string) string {

	incName = strings.Replace(incName, " ", "", -1)
	strPrefix := incName[:3]
	strSuffix := incName[len(incName)-3:]
	strName := strings.ToUpper(strPrefix + strSuffix)

	theID := incClientID + strName

	return theID
}

func GenerateAPIID(incClientID string, incUsername string) string {

	incUsername = strings.Replace(incUsername, " ", "", -1)
	strPrefix := incUsername[:4]
	strSuffix := incUsername[len(incUsername)-4:]
	strName := strings.ToUpper(strPrefix + strSuffix)

	theID := incClientID + strName

	return theID
}

func GenerateAPICredential(incClientID string) (string, string, string) {

	strClientID := incClientID[:6]

	rawUsername := GenerateRandomAlphabeticalString(12)
	strUsername := strClientID + rawUsername
	rawPassword := GenerateRandomAlphaNumericString(35)
	strPassword := rawPassword[10:]
	strKey := guuid.New().String()

	return strUsername, strPassword, strKey
}

func DoCheckGroupID(incGroupID string) bool {

	isValid := false
	intNumberX := int32(0)

	incPrefixSuffix := incGroupID[:6]
	incOutput := incGroupID[6:]

	incEnc := incOutput[:4]
	incNumber := incOutput[len(incEnc):]

	for _, c := range incPrefixSuffix {
		intNumberX = intNumberX + c
	}
	strNumberX := fmt.Sprintf("%v", intNumberX)

	_, encNumberX := SimpleStringEncrypt(strNumberX, Config.ConstDefaultGroupID)
	strEncX := strings.ToUpper(encNumberX[:4])

	if incNumber == strNumberX && incEnc == strEncX {
		isValid = true
	}

	return isValid
}

func DoCheckClientID(incClientID string) bool {

	isValid := false
	intNumberX := int32(0)

	incPrefixSuffix := incClientID[:6]
	incOutput := incClientID[6:]

	incEnc := incOutput[:3]
	incNumber := incOutput[len(incEnc):]

	for _, c := range incPrefixSuffix {
		intNumberX = intNumberX + c
	}
	strNumberX := fmt.Sprintf("%v", intNumberX)

	_, encNumberX := SimpleStringEncrypt(strNumberX, Config.ConstDefaultClientID)
	strEncX := strings.ToUpper(encNumberX[:3])

	if incNumber == strNumberX && incEnc == strEncX {
		isValid = true
	}

	return isValid
}

func DoCheckFormulaID(rc *redis.Client, cx context.Context, incRedis string) bool {

	isValid := false
	incRedisPattern := "*" + incRedis + "*"
	_, arrRedis := RedisKeysByPattern(rc, cx, incRedisPattern)

	if len(arrRedis) > 0 {
		isValid = true
	}
	return isValid
}

func DoCheckRedisClientHit(rc *redis.Client, cx context.Context, incClient string,
	incUsername string, incPassword string, incKey string, incRemoteIP string) bool {

	isValid := false
	isIPALLValid := false
	isIPAddressValid := false

	redisKey := Config.ConstRedisAPIHitKey + incClient
	redisVal, errR := RedisGet(rc, cx, redisKey)
	if errR == nil {
		//isRemoteIPValid := false
		mapRedisVal := ConvertJSONStringToMap("", redisVal)
		strUsername := GetStringFromMapInterface(mapRedisVal, "username")
		strPassword := GetStringFromMapInterface(mapRedisVal, "password")
		strKey := GetStringFromMapInterface(mapRedisVal, "key")
		strRemoteIP := GetStringFromMapInterface(mapRedisVal, "remoteip")

		isIPALLValid = strings.Contains(strRemoteIP, "ALL")
		isIPAddressValid = strings.Contains(strRemoteIP, incRemoteIP)

		if incUsername == strUsername && incPassword == strPassword && incKey == strKey {
			if isIPAddressValid {
				isValid = true
			} else if isIPALLValid {
				isValid = true
			} else {
				isValid = false
			}
		}
	}
	return isValid
}

//func ReloadFormulaToRedis(db *sql.DB, rc *redis.Client, cx context.Context, incFormulaID string) {
//
//	var arrF [50]string
//	var arrC []string
//
//	query := "SELECT f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, " +
//		"f11, f12, f13, f14, f15, f16, f17, f18, f19, f20," +
//		"f21, f22, f23, f24, f25, f26, f27, f28, f29, f30," +
//		"f31, f32, f33, f34, f35, f36, f37, f38, f39, f40," +
//		"f41, f42, f43, f44, f45, f46, f47, f48, f49, f50," +
//		"formula, client_id, formula_id, formula_type, formula_time " +
//		"FROM yformula WHERE is_active = true"
//
//	rows, err := db.Query(query)
//
//	if err != nil {
//		fmt.Println("FAILED : ", err)
//	} else {
//		arrC, _ = rows.Columns()
//
//		for rows.Next() {
//			var rawF [50]sql.NullString
//			var rawFormula sql.NullString
//			var rawClientID sql.NullString
//			var rawFormulaID sql.NullString
//			var rawType sql.NullString
//			var rawTime sql.NullTime
//
//			errP := rows.Scan(&rawF[0], &rawF[1], &rawF[2], &rawF[3], &rawF[4], &rawF[5], &rawF[6], &rawF[7], &rawF[8], &rawF[9], &rawF[10],
//				&rawF[11], &rawF[12], &rawF[13], &rawF[14], &rawF[15], &rawF[16], &rawF[17], &rawF[18], &rawF[19], &rawF[20],
//				&rawF[21], &rawF[22], &rawF[23], &rawF[24], &rawF[25], &rawF[26], &rawF[27], &rawF[28], &rawF[29], &rawF[30],
//				&rawF[31], &rawF[32], &rawF[33], &rawF[34], &rawF[35], &rawF[36], &rawF[37], &rawF[38], &rawF[39], &rawF[40],
//				&rawF[41], &rawF[42], &rawF[43], &rawF[44], &rawF[45], &rawF[46], &rawF[47], &rawF[48], &rawF[49],
//				&rawFormula, &rawClientID, &rawFormulaID, &rawType, &rawTime)
//
//			if errP != nil {
//				fmt.Println("FAILED : ", errP)
//			} else {
//				strFormula := ""
//				strFormula = ConvertSQLNullStringToString(rawFormula)
//				strClientID := ConvertSQLNullStringToString(rawClientID)
//				strFormulaID := ConvertSQLNullStringToString(rawFormulaID)
//				strType := ConvertSQLNullStringToString(rawType)
//				strTime := ""
//				if rawTime.Valid {
//					strTime = DoFormatDateTime("HH:mm", rawTime.Time)
//				}
//
//				//for x := 0; x < len(arrF); x++ {
//				for x := len(arrF) - 1; x >= 0; x-- {
//					arrF[x] = ConvertSQLNullStringToString(rawF[x])
//					if len(arrF[x]) > 0 {
//						strFormula = strings.Replace(strFormula, arrF[x], arrC[x], -1)
//					}
//				}
//
//				var strInsertAsResult []string
//				var strInsertAsFormula []string
//
//				var strInsertAsResultString []string
//
//				var strOutputHeader []string
//
//				var strOutputDataGroup []string
//				var strOutputRecapGroup []string
//
//				arrFormulas := strings.Split(strFormula, "@")
//				for x := 0; x < len(arrFormulas); x++ {
//					arrContent := strings.Split(arrFormulas[x], ":")
//
//					if len(arrFormulas[x]) > 1 && len(arrContent) > 1 {
//
//						incID := arrContent[0]
//						incParameter := strings.TrimLeft(strings.TrimRight(arrContent[1], " "), " ")
//
//						if strings.ToUpper(incID) == "STRING" {
//							fmt.Println(incParameter)
//							rawParameter := strings.Split(incParameter, "=")
//
//							for y := 0; y < len(rawParameter); y++ {
//
//								if y == 0 {
//									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
//									strInsertAsResultString = append(strInsertAsResultString, rawResults)
//								}
//							}
//
//						} else if strings.ToUpper(incID) == "FORMULA" {
//							fmt.Println(incParameter)
//							rawParameter := strings.Split(incParameter, "=")
//
//							for y := 0; y < len(rawParameter); y++ {
//
//								if y == 0 {
//									rawResults := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
//									strInsertAsResult = append(strInsertAsResult, rawResults)
//								}
//								if y == 1 {
//									rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[1], " "), " ")
//									strInsertAsFormula = append(strInsertAsFormula, rawFormulas)
//								}
//							}
//
//						} else if strings.ToUpper(incID) == "OUTPUTHEADER" {
//							fmt.Println(incParameter)
//							rawParameter := strings.Split(incParameter, "=")
//
//							for y := 0; y < len(rawParameter); y++ {
//
//								if y == 0 {
//									rawHeader := strings.Split(rawParameter[0], ",")
//
//									for z := 0; z < len(rawHeader); z++ {
//										rawHeaders := strings.TrimLeft(strings.TrimRight(rawHeader[z], " "), " ")
//										strOutputHeader = append(strOutputHeader, rawHeaders)
//									}
//								}
//							}
//
//						} else if strings.ToUpper(incID) == "OUTPUTDATAGROUP" {
//							fmt.Println(incParameter)
//							rawParameter := strings.Split(incParameter, "=")
//
//							for y := 0; y < len(rawParameter); y++ {
//
//								if y == 0 {
//									rawGroup := strings.Split(rawParameter[0], ",")
//
//									for z := 0; z < len(rawGroup); z++ {
//										rawGroups := strings.TrimLeft(strings.TrimRight(rawGroup[z], " "), " ")
//										strOutputDataGroup = append(strOutputDataGroup, rawGroups)
//									}
//								}
//							}
//
//						} else if strings.ToUpper(incID) == "OUTPUTRECAPGROUP" {
//							fmt.Println(incParameter)
//							rawParameter := strings.Split(incParameter, "=")
//
//							for y := 0; y < len(rawParameter); y++ {
//
//								if y == 0 {
//									rawFormulas := strings.TrimLeft(strings.TrimRight(rawParameter[0], " "), " ")
//									strOutputRecapGroup = append(strOutputRecapGroup, rawFormulas)
//								}
//							}
//						}
//					}
//				}
//
//				var mapRedis = make(map[string]interface{})
//				mapRedis["client"] = strClientID
//				mapRedis["formula"] = strInsertAsFormula
//				mapRedis["result"] = strInsertAsResult
//				mapRedis["string"] = strInsertAsResultString
//				mapRedis["header"] = strOutputHeader
//				mapRedis["data"] = strOutputDataGroup
//				mapRedis["recap"] = strOutputRecapGroup
//				mapRedis["type"] = strType
//				mapRedis["time"] = strTime
//				jsonRedis := ConvertMapInterfaceToJSON(mapRedis)
//
//				redisKey := strType + "_" + Config.ConstRedisKey + strFormulaID
//				errR := RedisSet(rc, cx, redisKey, jsonRedis, 0)
//				if errR == nil {
//					//fmt.Println("Success load : ", strFormulaID)
//				} else {
//					fmt.Println("Failed to load : ", strFormulaID)
//				}
//			}
//		}
//	}
//}

func ReloadFormulaToRedis(db *sql.DB, rc *redis.Client, cx context.Context, incFormulaID string) {

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
				//strFields := ConvertSQLNullStringToString(rawFields)
				//mapFields := ConvertJSONStringToMap("", strFields)
				strFormula := ""
				strFormula = ConvertSQLNullStringToString(rawFormula)
				strClientID := ConvertSQLNullStringToString(rawClientID)
				strFormulaID := ConvertSQLNullStringToString(rawFormulaID)
				strType := ConvertSQLNullStringToString(rawType)
				strTime := ""
				if rawTime.Valid {
					strTime = DoFormatDateTime("HH:mm", rawTime.Time)
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
				jsonRedis := ConvertMapInterfaceToJSON(mapRedis)

				redisKey := strType + "_" + Config.ConstRedisKey + strFormulaID
				//redisKey := "test_" + strFormulaID
				errR := RedisSet(rc, cx, redisKey, jsonRedis, 0)
				if errR == nil {
					//fmt.Println("Success load : ", strFormulaID)
				} else {
					fmt.Println("Failed to load : ", strFormulaID)
				}
			}
		}
	}
}

func RemoveFormulaFromRedis(db *sql.DB, rc *redis.Client, cx context.Context, incFormulaID string) {
	redisKey := "*" + Config.ConstRedisKey + incFormulaID
	RedisDeleteKeysByPattern(rc, cx, redisKey)
}
