package zipserver

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
	Key    string // Storage key of the zip file (mutually exclusive with File)
	File   string // Local file path (mutually exclusive with Key)
	Prefix string // Prefix/path where extracted files should be stored
	Limits *ExtractLimits
}

// ExtractResult contains the result of an extract operation
type ExtractResult struct {
	ExtractedFiles []ExtractedFile
	Err            error
}

// CopyParams contains parameters for the copy operation
type CopyParams struct {
	Key            string // Storage key to copy
	TargetName     string // Target storage name
	ExpectedBucket string // Optional: expected bucket for validation
}

// CopyResult contains the result of a copy operation
type CopyResult struct {
	Key      string
	Duration string
	Size     int64
	Md5      string
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
	Errors      []DeleteError
	Err         error
}

// ListParams contains parameters for the list operation
type ListParams struct {
	Key string // Storage key of the zip file (mutually exclusive with URL)
	URL string // URL of the zip file (mutually exclusive with Key)
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
}

// SlurpResult contains the result of a slurp operation
type SlurpResult struct {
	Err error
}
