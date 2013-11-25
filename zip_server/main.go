package main

import (
	"fmt"
	"log"
	"flag"
	"os"
	. "github.com/leafo/zip_server"
)

var _ fmt.Formatter

var (
	configFname string
	listenTo string
	dumpConfig bool
)

func init() {
	flag.StringVar(&configFname, "config", DefaultConfigFname, "Path to json config file")
	flag.StringVar(&listenTo, "listen", "127.0.0.1:8090", "Address to listen to")
	flag.BoolVar(&dumpConfig, "dump", false, "Dump the parsed config and exit")
}

func main() {
	flag.Parse()
	config := LoadConfig(configFname)

	if dumpConfig {
		fmt.Println(config.Dump())
		os.Exit(0)
	}

	if err := StartZipServer(listenTo, config); err != nil {
		log.Fatal(err)
	}
}

