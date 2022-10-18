package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/go-errors/errors"
	"github.com/itchio/zipserver/zipserver"
)

var _ fmt.Formatter

var (
	configFname string
	listenTo    string
	dumpConfig  bool
	serve       string
	extract     string
)

func init() {
	flag.StringVar(&configFname, "config", zipserver.DefaultConfigFname, "Path to json config file")
	flag.StringVar(&listenTo, "listen", "127.0.0.1:8090", "Address to listen to")
	flag.BoolVar(&dumpConfig, "dump", false, "Dump the parsed config and exit")
	flag.StringVar(&serve, "serve", "", "Serve a given zip from a local HTTP server")
	flag.StringVar(&extract, "extract", "", "Extract zip file to random name on GCS (requires a config with bucket)")
}

func must(err error) {
	if err == nil {
		return
	}

	if se, ok := err.(*errors.Error); ok {
		log.Fatal(se.ErrorStack())
	} else {
		log.Fatal(err.Error())
	}
}

func main() {
	flag.Parse()

	config, err := zipserver.LoadConfig(configFname)
	must(err)

	if dumpConfig {
		fmt.Println(config)
		return
	}

	if serve != "" {
		must(zipserver.ServeZip(config, serve))
		return
	}

	if extract != "" {
		archiver := zipserver.NewArchiver(config)
		limits := zipserver.DefaultExtractLimits(config)

		log.Println("Extraction threads:", limits.ExtractionThreads)
		log.Println("Bucket:", config.Bucket)

		rand.Seed(time.Now().UTC().UnixNano())

		var letters = []rune("rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz")

		var randChars = make([]rune, 20)
		for i := range randChars {
			randChars[i] = letters[rand.Intn(len(letters))]
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.JobTimeout))
		defer cancel()

		files, err := archiver.UploadZipFromFile(ctx, extract, string(randChars), limits)
		if err != nil {
			log.Fatal(err.Error())
			return
		}

		blob, _ := json.Marshal(struct {
			Success        bool
			ExtractedFiles []zipserver.ExtractedFile
		}{true, files})

		fmt.Println(string(blob))
		return
	}

	err = zipserver.StartZipServer(listenTo, config)
	must(err)
}
