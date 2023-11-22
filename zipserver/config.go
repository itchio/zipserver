package zipserver

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

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

	S3AccessKeyID string `json:",omitempty"`
	S3SecretKey   string `json:",omitempty"`
	S3Endpoint    string `json:",omitempty"`
	S3Bucket      string `json:",omitempty"`
	S3Region      string `json:",omitempty"`

	JobTimeout               Duration `json:",omitempty"` // Time to complete entire extract or upload job
	FileGetTimeout           Duration `json:",omitempty"` // Time to download a single object
	FilePutTimeout           Duration `json:",omitempty"` // Time to upload a single object
	AsyncNotificationTimeout Duration `json:",omitempty"` // Time to complete webhook request
}

var defaultConfig = Config{
	MaxFileSize:       1024 * 1024 * 200,
	MaxTotalSize:      1024 * 1024 * 500,
	MaxNumFiles:       100,
	MaxFileNameLength: 80,
	ExtractionThreads: 4,

	JobTimeout:               Duration(5 * time.Minute),
	FileGetTimeout:           Duration(1 * time.Minute),
	FilePutTimeout:           Duration(1 * time.Minute),
	AsyncNotificationTimeout: Duration(5 * time.Second),
}

// Duration adds JSON (de)serialization to time.Duration.
// This should be fixed in Go 2.
// https://github.com/golang/go/issues/10275
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// LoadConfig reads a config file into a config struct
func LoadConfig(fname string) (*Config, error) {
	jsonBlob, err := os.ReadFile(fname)
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
		return fmt.Sprintf("Error: could not stringify config: %s", err.Error())
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
