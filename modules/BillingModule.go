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

func ReloadFormulaToRedis(db *sql.DB, rc *redis.Client, cx context.Context, incFormulaID string) {

	var arrField [20]string
	var arrColumn []string

	if len(incFormulaID) == 0 {
		incFormulaID = "%"
	}

	query := "SELECT field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, " +
		"field11, field12, field13, field14, field15, field16, field17, field18, field19, field20," +
		"formula, client_id, formula_id, formula_type, formula_time " +
		"FROM formula WHERE formula_id LIKE $1 AND is_active = true"

	rows, err := db.Query(query, incFormulaID)

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
				strFormula = ConvertSQLNullStringToString(rawFormula)
				strClientID := ConvertSQLNullStringToString(rawClientID)
				strFormulaID := ConvertSQLNullStringToString(rawFormulaID)
				strType := ConvertSQLNullStringToString(rawType)
				strTime := ""
				if rawTime.Valid {
					strTime = DoFormatDateTime("HH:mm", rawTime.Time)
				}

				for x := 0; x < len(arrField); x++ {
					arrField[x] = ConvertSQLNullStringToString(rawField[x])
					if len(arrField[x]) > 0 {
						strFormula = strings.Replace(strFormula, arrField[x], arrColumn[x], -1)
					}
				}

				var mapRedis = make(map[string]interface{})
				mapRedis["client"] = strClientID
				mapRedis["formula"] = strFormula
				mapRedis["type"] = strType
				mapRedis["time"] = strTime
				jsonRedis := ConvertMapInterfaceToJSON(mapRedis)

				redisKey := Config.ConstRedisKey + strFormulaID
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
