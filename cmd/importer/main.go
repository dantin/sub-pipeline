package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"dantin/sub-pipeline/importer"
)

func main() {
	// Parse command line argument.
	cfg := importer.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags, %v", err)
		os.Exit(2)
	}

	// Create application and run.
	app := importer.NewImporter(cfg)
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
