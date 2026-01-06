package zipserver

import "fmt"

// Operations provides the core business logic for all zipserver operations.
// It can be used by both HTTP handlers and CLI commands.
type Operations struct {
	config *Config
}

// NewOperations creates a new Operations instance
func NewOperations(config *Config) *Operations {
	return &Operations{config: config}
}

// ExtractParams contains parameters for the extract operation
type ExtractParams struct {
	Key        string // Storage key of the zip file (mutually exclusive with File)
	File       string // Local file path (mutually exclusive with Key)
	Prefix     string // Prefix/path where extracted files should be stored
	Limits     *ExtractLimits
	TargetName string // Optional: target storage name for extracted files
}

// ExtractResult contains the result of an extract operation
type ExtractResult struct {
	ExtractedFiles []ExtractedFile
	Err            error
}

// CopyParams contains parameters for the copy operation
type CopyParams struct {
	Key            string // Storage key to copy
	DestKey        string // Optional: destination key (defaults to Key if empty)
	TargetName     string // Optional: target storage name (if empty, copies within primary storage)
	ExpectedBucket string // Optional: expected bucket for validation
	HtmlFooter     string // Optional: HTML to append to index.html files
}

// DestKeyOrKey returns the destination key, defaulting to Key when DestKey is empty.
func (p CopyParams) DestKeyOrKey() string {
	if p.DestKey == "" {
		return p.Key
	}
	return p.DestKey
}

// Validate checks copy parameter requirements including target validation.
func (p CopyParams) Validate(config *Config) error {
	if p.Key == "" {
		return fmt.Errorf("Key is required")
	}

	destKey := p.DestKeyOrKey()

	// Require either cross-storage copy (TargetName) or rename (DestKey != Key)
	if p.TargetName == "" && p.DestKey == "" {
		return fmt.Errorf("missing required parameter: target or dest_key")
	}
	if p.TargetName == "" && destKey == p.Key {
		return fmt.Errorf("dest_key must differ from key for same-storage copy")
	}

	// Validate target and expected bucket
	if p.TargetName == "" {
		// Same-storage copy: validate against primary bucket
		if p.ExpectedBucket != "" && p.ExpectedBucket != config.Bucket {
			return fmt.Errorf("expected bucket does not match primary bucket: %s != %s", p.ExpectedBucket, config.Bucket)
		}
	} else {
		// Cross-storage copy: validate target exists and is writable
		storageTargetConfig := config.GetStorageTargetByName(p.TargetName)
		if storageTargetConfig == nil {
			return fmt.Errorf("invalid target: %s", p.TargetName)
		}
		if storageTargetConfig.Readonly {
			return fmt.Errorf("target %s is readonly", p.TargetName)
		}
		if p.ExpectedBucket != "" && p.ExpectedBucket != storageTargetConfig.Bucket {
			return fmt.Errorf("expected bucket does not match target bucket: %s != %s", p.ExpectedBucket, storageTargetConfig.Bucket)
		}
	}

	return nil
}

// CopyResult contains the result of a copy operation
type CopyResult struct {
	Key      string
	Duration string
	Size     int64
	Md5      string
	Injected bool // true if HTML footer was injected
	Err      error
}

// DeleteParams contains parameters for the delete operation
type DeleteParams struct {
	Keys       []string // Storage keys to delete
	TargetName string   // Target storage name
}

// DeleteOperationResult contains the result of a delete operation
type DeleteOperationResult struct {
	TotalKeys   int
	DeletedKeys int
	Duration    string
	Errors      []DeleteError
	Err         error
}

// ListParams contains parameters for the list operation
type ListParams struct {
	Key  string // Storage key of the zip file (mutually exclusive with URL/File)
	URL  string // URL of the zip file (mutually exclusive with Key/File)
	File string // Local path to zip file (mutually exclusive with Key/URL)
}

// ListResult contains the result of a list operation
type ListResult struct {
	Files []fileTuple
	Err   error
}

// SlurpParams contains parameters for the slurp operation
type SlurpParams struct {
	Key                string // Storage key to save as
	URL                string // URL to download
	ContentType        string // Optional: content type override
	MaxBytes           uint64 // Optional: maximum bytes to download
	ACL                string // Optional: ACL for the uploaded file
	ContentDisposition string // Optional: content disposition header
	TargetName         string // Optional: target storage name for uploaded file
}

// SlurpResult contains the result of a slurp operation
type SlurpResult struct {
	Err error
}
