package zip_server

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

var DefaultConfigFname = "zip_server.json"

type Config struct {
	PrivateKeyPath string
	ClientEmail string
}

func LoadConfig(fname string) *Config {
	jsonBlob, err := ioutil.ReadFile(fname)

	if err != nil {
		log.Fatal(err)
	}

	config := &Config{}
	err = json.Unmarshal(jsonBlob, config)

	if err != nil {
		log.Fatal("Failed parsing config: " + fname + ": " + err.Error())
	}

	return config
}

