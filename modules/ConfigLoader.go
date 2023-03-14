package modules

func LoadConfig() map[string]string {
	mapConfig := make(map[string]string)

	// Local
	mapConfig["databaseHost"] = "localhost"
	mapConfig["databasePort"] = "5432"
	mapConfig["databaseName"] = "billing_formula"
	//mapConfig["databaseName"] = "pcubilling"
	mapConfig["databaseUser"] = "postgres"
	mapConfig["databasePass"] = "postgres"

	mapConfig["redisHost"] = "localhost"
	mapConfig["redisPort"] = "6379"
	mapConfig["redisDB"] = "3"
	mapConfig["redisUser"] = ""
	mapConfig["redisPass"] = "ading"

	mapConfig["rabbitHost"] = "localhost"
	mapConfig["rabbitPort"] = "5672"
	mapConfig["rabbitUser"] = "guest"
	mapConfig["rabbitPass"] = "guest"
	mapConfig["rabbitVHost"] = "QPAY"

	mapConfig["mongoDBHost"] = "localhost"
	mapConfig["mongoDBPort"] = "27017"

	return mapConfig
}

func LoadConfigProduction() map[string]string {
	mapConfig := make(map[string]string)

	// Production
	mapConfig["databaseHost"] = "localhost"
	mapConfig["databasePort"] = "5432"
	mapConfig["databaseName"] = "settlement"
	mapConfig["databaseUser"] = "bot3c"
	mapConfig["databasePass"] = "CakepBanget123!"
	//mapConfig["databaseHost"] = "localhost"
	//mapConfig["databasePort"] = "5432"
	//mapConfig["databaseName"] = "theformula"
	//mapConfig["databaseName"] = "pcubilling"
	//mapConfig["databaseUser"] = "postgres"
	//mapConfig["databasePass"] = "123456"

	mapConfig["redisHost"] = "localhost"
	mapConfig["redisPort"] = "6379"
	mapConfig["redisDB"] = "10"
	mapConfig["redisUser"] = ""
	mapConfig["redisPass"] = "Eliandri3"

	mapConfig["rabbitHost"] = "localhost"
	mapConfig["rabbitPort"] = "5672"
	mapConfig["rabbitUser"] = "chandra"
	mapConfig["rabbitPass"] = "CakepBanget123!"
	mapConfig["rabbitVHost"] = "SETTLEMENT"

	mapConfig["mongoDBHost"] = "localhost"
	mapConfig["mongoDBPort"] = "27017"

	return mapConfig
}
