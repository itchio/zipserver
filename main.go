package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/leafo/zipserver/zipserver"
)

var _ fmt.Formatter

var (
	configFname string
	listenTo    string
	dumpConfig  bool
)

func init() {
	flag.StringVar(&configFname, "config", zipserver.DefaultConfigFname, "Path to json config file")
	flag.StringVar(&listenTo, "listen", "127.0.0.1:8090", "Address to listen to")
	flag.BoolVar(&dumpConfig, "dump", false, "Dump the parsed config and exit")
}

func main() {
	flag.Parse()
	config, err := zipserver.LoadConfig(configFname)
	if err != nil {
		log.Fatal(err)
	}

	if dumpConfig {
		fmt.Println(config)
		os.Exit(0)
	}

	err = zipserver.StartZipServer(listenTo, config)
	if err != nil {
		log.Fatal(err)
	}
}
