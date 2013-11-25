package main

import (
	"fmt"
	"flag"
	. "github.com/leafo/zip_server"
)

var _ fmt.Formatter

var (
	configFname string
	listenTo string
)

func init() {
	flag.StringVar(&configFname, "config", DefaultConfigFname, "Path to json config file")
	flag.StringVar(&listenTo, "listen", "127.0.0.1:8081", "Address to listen to")
}

func main() {
	flag.Parse()
	config := LoadConfig(configFname)
	StartZipServer(listenTo, config)
}

