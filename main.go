package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
)

var (
	confFile   = flag.String("conf", "rabbitmqperfcfg.yaml", "The path to the file (yaml) defining perf tests to run.")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func printUsageAndBail(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:")
	flag.PrintDefaults()
	os.Exit(64)
}

func main() {
	flag.Parse()

	if *confFile == "" {
		printUsageAndBail("No test definition provided.")
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logger.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	err := runConfigFile(*confFile)
	if err != nil {
		logger.Printf("error: %v", err)
	}
}
