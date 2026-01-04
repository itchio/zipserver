package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/itchio/zipserver/zipserver"
)

// Build-time variables set via ldflags
var (
	Version   = "dev"
	CommitSHA = "unknown"
	BuildTime = "unknown"
)

var (
	app = kingpin.New("zipserver", "A zip file extraction and storage service")

	// Global flags
	configFile = app.Flag("config", "Path to config file").
			Default(zipserver.DefaultConfigFname).
			Short('c').
			String()
	threads = app.Flag("threads", "Number of threads for parallel operations").
		Short('t').
		Int()

	// Server command (default behavior)
	serverCmd    = app.Command("server", "Start HTTP server").Default()
	serverListen = serverCmd.Flag("listen", "Address to listen on").
			Default("127.0.0.1:8090").
			String()

	// Extract command
	extractCmd          = app.Command("extract", "Extract a zip file to storage")
	extractKey          = extractCmd.Flag("key", "Storage key of the zip file").String()
	extractFile         = extractCmd.Flag("file", "Local path to zip file").String()
	extractPrefix       = extractCmd.Flag("prefix", "Prefix for extracted files").Required().String()
	extractTarget       = extractCmd.Flag("target", "Target storage name for extracted files").String()
	extractMaxFileSize  = extractCmd.Flag("max-file-size", "Maximum size per file in bytes").Uint64()
	extractMaxTotalSize = extractCmd.Flag("max-total-size", "Maximum total extracted size in bytes").Uint64()
	extractMaxNumFiles  = extractCmd.Flag("max-num-files", "Maximum number of files").Int()
	extractFilter       = extractCmd.Flag("filter", "Glob pattern to filter extracted files (e.g., '*.png', 'assets/**/*.js')").String()

	// Copy command
	copyCmd    = app.Command("copy", "Copy a file to target storage")
	copyKey    = copyCmd.Flag("key", "Storage key to copy").Required().String()
	copyTarget = copyCmd.Flag("target", "Target storage name").Required().String()
	copyBucket = copyCmd.Flag("bucket", "Expected bucket (optional verification)").String()

	// Delete command
	deleteCmd    = app.Command("delete", "Delete files from storage")
	deleteKeys   = deleteCmd.Flag("key", "Storage keys to delete (can be specified multiple times)").Required().Strings()
	deleteTarget = deleteCmd.Flag("target", "Target storage name").Required().String()

	// List command
	listCmd = app.Command("list", "List files in a zip archive")
	listKey = listCmd.Flag("key", "Storage key of the zip file").String()
	listURL = listCmd.Flag("url", "URL of the zip file").String()

	// Slurp command
	slurpCmd                = app.Command("slurp", "Download URL and store in storage")
	slurpKey                = slurpCmd.Flag("key", "Storage key to save as").Required().String()
	slurpURL                = slurpCmd.Flag("url", "URL to download").Required().String()
	slurpContentType        = slurpCmd.Flag("content-type", "Content type").String()
	slurpMaxBytes           = slurpCmd.Flag("max-bytes", "Maximum bytes to download").Uint64()
	slurpACL                = slurpCmd.Flag("acl", "ACL for the uploaded file").String()
	slurpContentDisposition = slurpCmd.Flag("content-disposition", "Content disposition header").String()

	// Testzip command (serves a local zip file via HTTP for debugging)
	testzipCmd  = app.Command("testzip", "Extract and serve a local zip file via HTTP for debugging")
	testzipFile = testzipCmd.Arg("file", "Path to zip file").Required().String()

	// Dump command
	dumpCmd = app.Command("dump", "Dump parsed config and exit")

	// Version command
	versionCmd = app.Command("version", "Print version information")
)

func must(err error) {
	if err == nil {
		return
	}

	log.Fatal(err)
}

func outputJSON(v interface{}) {
	blob, err := json.Marshal(v)
	if err != nil {
		log.Fatal("Failed to marshal JSON:", err)
	}
	fmt.Println(string(blob))
}

func main() {
	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Handle version early (no config needed)
	if cmd == versionCmd.FullCommand() {
		fmt.Printf("zipserver %s\n", Version)
		fmt.Printf("  commit: %s\n", CommitSHA)
		fmt.Printf("  built:  %s\n", BuildTime)
		return
	}

	config, err := zipserver.LoadConfig(*configFile)
	must(err)

	if *threads > 0 {
		config.ExtractionThreads = *threads
	}

	switch cmd {
	case serverCmd.FullCommand():
		runServer(config)
	case extractCmd.FullCommand():
		runExtract(config)
	case copyCmd.FullCommand():
		runCopy(config)
	case deleteCmd.FullCommand():
		runDelete(config)
	case listCmd.FullCommand():
		runList(config)
	case slurpCmd.FullCommand():
		runSlurp(config)
	case testzipCmd.FullCommand():
		runTestzip(config)
	case dumpCmd.FullCommand():
		fmt.Println(config)
	}
}

func runServer(config *zipserver.Config) {
	err := zipserver.StartZipServer(*serverListen, config)
	must(err)
}

func runExtract(config *zipserver.Config) {
	if *extractKey == "" && *extractFile == "" {
		log.Fatal("Either --key or --file must be specified")
	}
	if *extractKey != "" && *extractFile != "" {
		log.Fatal("Only one of --key or --file can be specified")
	}

	ops := zipserver.NewOperations(config)

	limits := zipserver.DefaultExtractLimits(config)
	if *extractMaxFileSize > 0 {
		limits.MaxFileSize = *extractMaxFileSize
	}
	if *extractMaxTotalSize > 0 {
		limits.MaxTotalSize = *extractMaxTotalSize
	}
	if *extractMaxNumFiles > 0 {
		limits.MaxNumFiles = *extractMaxNumFiles
	}
	if *extractFilter != "" {
		limits.IncludeGlob = *extractFilter
	}

	params := zipserver.ExtractParams{
		Key:        *extractKey,
		File:       *extractFile,
		Prefix:     *extractPrefix,
		Limits:     limits,
		TargetName: *extractTarget,
	}

	log.Println("Extraction threads:", limits.ExtractionThreads)
	log.Println("Source bucket:", config.Bucket)
	if *extractTarget != "" {
		targetConfig := config.GetStorageTargetByName(*extractTarget)
		if targetConfig == nil {
			log.Fatalf("invalid target: %s", *extractTarget)
		}
		log.Println("Target:", *extractTarget)
		log.Println("Target bucket:", targetConfig.Bucket)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
	defer cancel()

	result := ops.Extract(ctx, params)
	if result.Err != nil {
		log.Fatal(result.Err)
	}

	outputJSON(struct {
		Success        bool
		ExtractedFiles []zipserver.ExtractedFile
	}{true, result.ExtractedFiles})
}

func runCopy(config *zipserver.Config) {
	ops := zipserver.NewOperations(config)

	params := zipserver.CopyParams{
		Key:            *copyKey,
		TargetName:     *copyTarget,
		ExpectedBucket: *copyBucket,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
	defer cancel()

	result := ops.Copy(ctx, params)
	if result.Err != nil {
		log.Fatal(result.Err)
	}

	outputJSON(struct {
		Success  bool
		Key      string
		Duration string
		Size     int64
		Md5      string
	}{true, result.Key, result.Duration, result.Size, result.Md5})
}

func runDelete(config *zipserver.Config) {
	ops := zipserver.NewOperations(config)

	params := zipserver.DeleteParams{
		Keys:       *deleteKeys,
		TargetName: *deleteTarget,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
	defer cancel()

	result := ops.Delete(ctx, params)
	if result.Err != nil {
		log.Fatal(result.Err)
	}

	outputJSON(struct {
		Success     bool
		TotalKeys   int
		DeletedKeys int
		Errors      []zipserver.DeleteError `json:",omitempty"`
	}{len(result.Errors) == 0, result.TotalKeys, result.DeletedKeys, result.Errors})
}

func runList(config *zipserver.Config) {
	if *listKey == "" && *listURL == "" {
		log.Fatal("Either --key or --url must be specified")
	}
	if *listKey != "" && *listURL != "" {
		log.Fatal("Only one of --key or --url can be specified")
	}

	ops := zipserver.NewOperations(config)

	params := zipserver.ListParams{
		Key: *listKey,
		URL: *listURL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.FileGetTimeout))
	defer cancel()

	result := ops.List(ctx, params)
	if result.Err != nil {
		log.Fatal(result.Err)
	}

	outputJSON(result.Files)
}

func runSlurp(config *zipserver.Config) {
	ops := zipserver.NewOperations(config)

	params := zipserver.SlurpParams{
		Key:                *slurpKey,
		URL:                *slurpURL,
		ContentType:        *slurpContentType,
		MaxBytes:           *slurpMaxBytes,
		ACL:                *slurpACL,
		ContentDisposition: *slurpContentDisposition,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
	defer cancel()

	result := ops.Slurp(ctx, params)
	if result.Err != nil {
		log.Fatal(result.Err)
	}

	outputJSON(struct {
		Success bool
	}{true})
}

func runTestzip(config *zipserver.Config) {
	must(zipserver.ServeZip(config, *testzipFile))
}
