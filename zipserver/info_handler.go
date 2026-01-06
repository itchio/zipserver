package zipserver

import (
	"context"
	"fmt"
)

// Info retrieves metadata headers for a file in storage
func (o *Operations) Info(ctx context.Context, params InfoParams) InfoResult {
	if params.Key == "" {
		return InfoResult{Err: fmt.Errorf("key is required")}
	}

	var storage Storage
	var bucket string
	var err error

	if params.TargetName == "" {
		// Primary storage
		storage, err = NewGcsStorage(o.config)
		if err != nil {
			return InfoResult{Err: fmt.Errorf("failed to create primary storage: %v", err)}
		}
		bucket = o.config.Bucket
	} else {
		// Named target storage
		storageTargetConfig := o.config.GetStorageTargetByName(params.TargetName)
		if storageTargetConfig == nil {
			return InfoResult{Err: fmt.Errorf("invalid target: %s", params.TargetName)}
		}

		storage, err = storageTargetConfig.NewStorageClient()
		if err != nil {
			return InfoResult{Err: fmt.Errorf("failed to create target storage: %v", err)}
		}
		bucket = storageTargetConfig.Bucket
	}

	// Check if storage supports HeadFile
	headable, ok := storage.(HeadableStorage)
	if !ok {
		return InfoResult{Err: fmt.Errorf("storage does not support info operation")}
	}

	headers, err := headable.HeadFile(ctx, bucket, params.Key)
	if err != nil {
		return InfoResult{Err: fmt.Errorf("failed to get file info: %v", err)}
	}

	// Convert http.Header to map[string][]string for JSON output
	headerMap := make(map[string][]string)
	for key, values := range headers {
		if len(values) > 0 {
			copied := make([]string, len(values))
			copy(copied, values)
			headerMap[key] = copied
		}
	}

	return InfoResult{
		Key:     params.Key,
		Bucket:  bucket,
		Headers: headerMap,
	}
}
