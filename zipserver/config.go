package zipserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	errors "github.com/go-errors/errors"
)

// DefaultConfigFname is the default name for zipserver's config file
var DefaultConfigFname = "zipserver.json"

// ExtractLimits describes various limits we enforce when extracting zips,
// mostly related to the number of files, their sizes, and the lengths of their paths
type ExtractLimits struct {
	MaxFileSize       uint64
	MaxTotalSize      uint64
	MaxNumFiles       int
	MaxFileNameLength int
	ExtractionThreads int
}

// Config contains both storage configuration and the enforced extraction limits
type Config struct {
	PrivateKeyPath string
	ClientEmail    string
	Bucket         string
	ExtractPrefix  string

	MaxFileSize       uint64
	MaxTotalSize      uint64
	MaxNumFiles       int
	MaxFileNameLength int
	ExtractionThreads int
}

var defaultConfig = Config{
	MaxFileSize:       1024 * 1024 * 200,
	MaxTotalSize:      1024 * 1024 * 500,
	MaxNumFiles:       100,
	MaxFileNameLength: 80,
	ExtractionThreads: 4,
}

// LoadConfig reads a config file into a config struct
func LoadConfig(fname string) (*Config, error) {
	jsonBlob, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	config := defaultConfig
	err = json.Unmarshal(jsonBlob, &config)

	if err != nil {
		return nil, fmt.Errorf("Failed parsing config file %s: %s", fname, err.Error())
	}

	if config.PrivateKeyPath == "" {
		return nil, errors.New("Config error: PrivateKeyPath field missing")
	}

	if config.ClientEmail == "" {
		return nil, errors.New("Config error: ClientEmail field missing")
	}

	if config.Bucket == "" {
		return nil, errors.New("Config error: Bucket field missing")
	}

	if config.ExtractPrefix == "" {
		return nil, errors.New("Config error: ExtractPrefix field missing")
	}

	return &config, nil
}

func (c *Config) String() string {
	bytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	return string(bytes)
}

// DefaultExtractLimits returns only extract limits from a config struct
func DefaultExtractLimits(config *Config) *ExtractLimits {
	return &ExtractLimits{
		MaxFileSize:       config.MaxFileSize,
		MaxTotalSize:      config.MaxTotalSize,
		MaxNumFiles:       config.MaxNumFiles,
		MaxFileNameLength: config.MaxFileNameLength,
		ExtractionThreads: config.ExtractionThreads,
	}
}
