package zipserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// resolveCredentialPath resolves a credential file path.
// If CREDENTIALS_DIRECTORY is set and the path is not absolute,
// it checks the credentials directory first (for systemd LoadCredential support).
func resolveCredentialPath(path string) string {
	if path == "" {
		return path
	}

	credDir := os.Getenv("CREDENTIALS_DIRECTORY")
	if credDir == "" || filepath.IsAbs(path) {
		return path
	}

	// Check if file exists in credentials directory
	credPath := filepath.Join(credDir, filepath.Base(path))
	if _, err := os.Stat(credPath); err == nil {
		return credPath
	}

	return path
}

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
	IncludeGlob       string   // Glob pattern for file inclusion (empty = all files)
	OnlyFiles         []string // Exact file paths to extract (mutually exclusive with IncludeGlob)
	MaxInputZipSize   uint64
	HtmlFooter        string // HTML snippet to append to index.html files
}

type StorageType int

const (
	GCS StorageType = iota // Google Cloud Storage
	S3                     // Amazon S3 Storage
	Mem                    // Memory Storage (for testing)
)

var storageTypeString = map[string]StorageType{
	"GCS": GCS,
	"S3":  S3,
	"Mem": Mem,
}

var storageTypeInt = map[StorageType]string{
	GCS: "GCS",
	S3:  "S3",
	Mem: "Mem",
}

func (s *StorageType) MarshalJSON() ([]byte, error) {
	return json.Marshal(storageTypeInt[*s])
}

func (s *StorageType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	val, ok := storageTypeString[str]
	if !ok {
		return errors.New("Invalid StorageType value")
	}
	*s = val
	return nil
}

// StorageTarget represents a storage configuration that can be written to, either GCS or S3
type StorageConfig struct {
	Name string

	Type     StorageType
	Readonly bool `json:",omitempty"`

	// GCS target credentials (service account PEM key + client email)
	GCSPrivateKeyPath string `json:",omitempty"`
	GCSClientEmail    string `json:",omitempty"`

	S3AccessKeyID string `json:",omitempty"`
	S3SecretKey   string `json:",omitempty"`
	S3Endpoint    string `json:",omitempty"`
	S3Region      string `json:",omitempty"`

	Bucket string `json:",omitempty"`

	// ExtractPrefix overrides the global extract prefix for this target.
	// Empty inherits Config.ExtractPrefix. Use "." or "/" to extract at the bucket root.
	ExtractPrefix string `json:",omitempty"`

	// Compression settings for files extracted to this target.
	CompressEnabled    bool     `json:",omitempty"`
	CompressExtensions []string `json:",omitempty"`
	// CompressMinSize is the minimum file size before compression is attempted.
	// A nil pointer inherits the default; an explicit 0 means "no minimum".
	CompressMinSize *int64 `json:",omitempty"`
	CompressLevel   int    `json:",omitempty"`
}

// TODO: eventually this should be a factory that can return different storage types
func (sc *StorageConfig) NewStorageClient() (Storage, error) {
	switch sc.Type {
	case S3:
		return NewS3Storage(sc)
	case GCS:
		return NewGcsStorageWithCredentials(sc.GCSPrivateKeyPath, sc.GCSClientEmail)
	case Mem:
		// Use named storage if a name is provided, allowing tests to access the same instance
		if sc.Name != "" {
			return GetNamedMemStorage(sc.Name), nil
		}
		return NewMemStorage()
	default:
		return nil, fmt.Errorf("unsupported storage type")
	}
}

func (s *StorageConfig) Validate() error {
	if s.Name == "" {
		return errors.New("Config error: Name field missing")
	}

	missingFieldError := func(field string) error {
		return errors.New(fmt.Sprintf("Config error: [Storage %s] %s field missing", s.Name, field))
	}

	if s.Type == GCS {
		if s.GCSPrivateKeyPath == "" {
			return missingFieldError("GCSPrivateKeyPath")
		}

		if s.GCSClientEmail == "" {
			return missingFieldError("GCSClientEmail")
		}
	} else if s.Type == S3 {
		// access key and secret key are optional for S3, since they can be loaded from env
		if s.S3Endpoint == "" {
			return missingFieldError("S3Endpoint")
		}

		if s.S3Region == "" {
			return missingFieldError("S3Region")
		}
	} else if s.Type == Mem {
		// Mem storage doesn't require any credentials
	}

	if s.Bucket == "" {
		return missingFieldError("Bucket")
	}

	return nil
}

// Config contains both storage configuration and the enforced extraction limits
type Config struct {
	PrivateKeyPath string
	ClientEmail    string
	Bucket         string
	ExtractPrefix  string
	MetricsHost    string `json:",omitempty"`

	MaxFileSize       uint64
	MaxTotalSize      uint64
	MaxNumFiles       int
	MaxFileNameLength int
	ExtractionThreads int
	MaxInputZipSize   uint64
	MaxListFiles      int
	MaxPeekBytes      uint64 `json:",omitempty"`

	// CompressMaxConcurrent caps how many files may be gzip-compressed at once
	// across the whole process — a budget for how many cores we're willing to
	// spend on compression on a shared machine.
	CompressMaxConcurrent int `json:",omitempty"`

	JobTimeout               Duration `json:",omitempty"` // Time to complete entire extract or upload job
	FileGetTimeout           Duration `json:",omitempty"` // Time to download a single object
	FilePutTimeout           Duration `json:",omitempty"` // Time to upload a single object
	AsyncNotificationTimeout Duration `json:",omitempty"` // Time to complete webhook request

	// Places that can be written to
	StorageTargets []StorageConfig `json:",omitempty"`

	// Version info (set at runtime, not from config file)
	Version   string `json:"-"`
	CommitSHA string `json:"-"`
	BuildTime string `json:"-"`

	// compressLimiter is the lazily-built, process-wide compression concurrency
	// budget for this config (see getCompressLimiter). Unexported so it is not
	// serialized and adds no lock to Config, keeping the struct copy-safe.
	compressLimiter *compressLimiter
}

type CompressionConfig struct {
	Enabled    bool
	Extensions []string
	MinSize    int64
	Level      int
}

var defaultCompressExtensions = []string{".html", ".js", ".css", ".svg", ".wasm", ".wav", ".glb", ".pck", ".json", ".mem", ".gltf", ".data", ".symbols", ".ttf", ".otf", ".map", ".xml", ".txt", ".symbolmap", ".obj", ".bin"}

// GetStorageTargetByName returns the storage target with the given name from the config.
// If no such target exists, it returns nil.
func (c *Config) GetStorageTargetByName(name string) *StorageConfig {
	for i, target := range c.StorageTargets {
		if target.Name == name {
			return &c.StorageTargets[i]
		}
	}
	return nil
}

var defaultConfig = Config{
	MaxFileSize:       1024 * 1024 * 200,
	MaxTotalSize:      1024 * 1024 * 500,
	MaxNumFiles:       100,
	MaxFileNameLength: 255,
	ExtractionThreads: 4,
	MaxInputZipSize:   1024 * 1024 * 100,
	MaxListFiles:      50000,
	MaxPeekBytes:      1024 * 1024,

	CompressMaxConcurrent: defaultCompressMaxConcurrent,

	JobTimeout:               Duration(5 * time.Minute),
	FileGetTimeout:           Duration(1 * time.Minute),
	FilePutTimeout:           Duration(1 * time.Minute),
	AsyncNotificationTimeout: Duration(5 * time.Second),
}

func (s *StorageConfig) CompressionConfig() *CompressionConfig {
	if s == nil {
		return nil
	}
	// A nil CompressMinSize inherits the default; an explicit value (including 0,
	// meaning "no minimum") is used as-is.
	minSize := int64(defaultCompressMinSize)
	if s.CompressMinSize != nil {
		minSize = *s.CompressMinSize
	}
	config := &CompressionConfig{
		Enabled:    s.CompressEnabled,
		Extensions: s.CompressExtensions,
		MinSize:    minSize,
		Level:      s.CompressLevel,
	}
	if len(config.Extensions) == 0 {
		config.Extensions = defaultCompressExtensions
	}
	if config.Level == 0 {
		config.Level = defaultCompressLevel
	}
	return config
}

func (s *StorageConfig) EffectiveExtractPrefix(globalPrefix string) string {
	if s == nil || s.ExtractPrefix == "" {
		return globalPrefix
	}
	if s.ExtractPrefix == "." || s.ExtractPrefix == "/" {
		return ""
	}
	return s.ExtractPrefix
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
		return nil, err
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

	// validate storage targets
	for _, target := range config.StorageTargets {
		if err := target.Validate(); err != nil {
			return nil, err
		}
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
		MaxInputZipSize:   config.MaxInputZipSize,
	}
}
