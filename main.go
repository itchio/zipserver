package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/go-errors/errors"
	"github.com/leafo/zipserver/zipserver"
)

var _ fmt.Formatter

var (
	configFname string
	listenTo    string
	dumpConfig  bool
	serve       string
)

func init() {
	flag.StringVar(&configFname, "config", zipserver.DefaultConfigFname, "Path to json config file")
	flag.StringVar(&listenTo, "listen", "127.0.0.1:8090", "Address to listen to")
	flag.BoolVar(&dumpConfig, "dump", false, "Dump the parsed config and exit")
	flag.StringVar(&serve, "serve", "", "Serve a given zip from a local HTTP server")
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

	if serve != "" {
		must(zipserver.ServeZip(serve))
		return
	}

	config, err := zipserver.LoadConfig(configFname)
	must(err)

	if dumpConfig {
		fmt.Println(config)
		return
	}

	err = zipserver.StartZipServer(listenTo, config)
	must(err)
}
