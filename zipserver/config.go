package zipserver

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

var DefaultConfigFname = "zipserver.json"

type Config struct {
	PrivateKeyPath    string
	ClientEmail       string
	Bucket            string
	ExtractPrefix     string
	MaxFileSize       int
	MaxTotalSize      int
	MaxNumFiles       int
	MaxFileNameLength int
}

var defaultConfig = Config{
	MaxFileSize:       1024 * 1024 * 200,
	MaxTotalSize:      1024 * 1024 * 500,
	MaxNumFiles:       100,
	MaxFileNameLength: 80,
}

type ExtractLimits struct {
	MaxFileSize       int
	MaxTotalSize      int
	MaxNumFiles       int
	MaxFileNameLength int
}

func LoadConfig(fname string) *Config {
	jsonBlob, err := ioutil.ReadFile(fname)

	if err != nil {
		log.Fatal(err)
	}

	config := defaultConfig
	err = json.Unmarshal(jsonBlob, &config)

	if err != nil {
		log.Fatal("Failed parsing config: " + fname + ": " + err.Error())
	}

	return &config
}

func (c *Config) Dump() string {
	bytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	return string(bytes)
}

func DefaultExtractLimits(config *Config) *ExtractLimits {
	return &ExtractLimits{
		MaxFileSize:       config.MaxFileSize,
		MaxTotalSize:      config.MaxTotalSize,
		MaxNumFiles:       config.MaxNumFiles,
		MaxFileNameLength: config.MaxFileNameLength,
	}
}
