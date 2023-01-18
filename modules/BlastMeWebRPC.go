package modules

import (
	b64 "encoding/base64"
	"fmt"
	"strings"
)

const encSalt = "SayaBisaKarenaKebaikanTuhanSaja" // Harus 32 chars

func ReverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func CopyReversedSubStringToString(origString string, startPosFromSource int, endPosFromSource int, destString string, destPosition int) (bool, string) {
	isSuccess := false
	theResult := ""
	origRune := []rune(origString)
	destRune := []rune(destString)

	//fmt.Printf("origString: %s, startPosFromSource: %d, endPosFromSource: %d, origRune: %d\n", origString, startPosFromSource, endPosFromSource, len(origRune))
	//fmt.Printf("destString: %s, destPosition: %d\n", destString, destPosition)

	if startPosFromSource < 0 && endPosFromSource > (len(origRune)) && startPosFromSource <= endPosFromSource {
		fmt.Println("POSs noting is valid bro.")
		isSuccess = false
	} else {
		cutOrigString := string(origRune[startPosFromSource:endPosFromSource])
		reversedCutOrigString := ReverseString(cutOrigString)
		//fmt.Println("cutOrigString: " + cutOrigString + ", reversedCutOrigString: " + reversedCutOrigString)

		depanDestRune := string(destRune[0:destPosition])
		belakangDestRune := string(destRune[destPosition:len(destString)])
		theResult = depanDestRune + reversedCutOrigString + belakangDestRune

		isSuccess = true

		//fmt.Printf("depanRune: %s, belakangRune: %s, completeDestRune: %s\n", depanDestRune, belakangDestRune, theResult)
	}

	return isSuccess, theResult
}

func DeleteSubStringByIndex(theString string, index int, length int) string {
	theRune := []rune(theString)

	theFinalString := string(theRune[0:index]) + string(theRune[index+length:])

	return theFinalString
}

func SimpleStringEncrypt(theString string, theSalt string) (bool, string) {
	byteString := []byte(theString + theSalt)

	origB64enc := b64.StdEncoding.EncodeToString(byteString)
	destB64enc := origB64enc

	// move reversed string from 2 - 5 to pos 10
	isOK, result := CopyReversedSubStringToString(origB64enc, 2, 5, destB64enc, 10)

	// Replace = to _ in result
	result = strings.Replace(result, "=", "_dRP", 1)

	return isOK, result
}

func SimpleStringDecrypt(theEncString string, theSalt string) string {
	// Remove pos 10, 4 chars
	cleanEncString := DeleteSubStringByIndex(theEncString, 10, 3)
	cleanEncString = strings.Replace(cleanEncString, "_dRP", "=", 1)
	//fmt.Println("cleanES: " + cleanEncString)

	data, err := b64.StdEncoding.DecodeString(cleanEncString)
	if err != nil {
		return ""
	}

	rawData := string(data)
	cleanData := strings.Replace(rawData, theSalt, "", 1)

	return cleanData
}
