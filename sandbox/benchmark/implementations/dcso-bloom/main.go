// CLI wrapper for DCSO/bloom
// https://github.com/DCSO/bloom
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/DCSO/bloom"
)

func main() {
	var (
		createCmd = flag.NewFlagSet("create", flag.ExitOnError)
		addCmd    = flag.NewFlagSet("add", flag.ExitOnError)
		checkCmd  = flag.NewFlagSet("check", flag.ExitOnError)
		infoCmd   = flag.NewFlagSet("info", flag.ExitOnError)

		// create flags
		createCapacity = createCmd.Uint("n", 1000000, "capacity (number of items)")
		createFPP      = createCmd.Float64("p", 0.01, "false positive probability")
		createOutput   = createCmd.String("o", "filter.bloom", "output file")

		// add flags
		addFilter = addCmd.String("f", "filter.bloom", "filter file")
		addInput  = addCmd.String("i", "", "input file (stdin if not specified)")
		addOutput = addCmd.String("o", "", "output file (overwrites input if not specified)")

		// check flags
		checkFilter = checkCmd.String("f", "filter.bloom", "filter file")
		checkInput  = checkCmd.String("i", "", "input file (stdin if not specified)")

		// info flags
		infoFilter = infoCmd.String("f", "filter.bloom", "filter file")
	)

	if len(os.Args) < 2 {
		fmt.Println("Usage: dcso-bloom-cli <command> [options]")
		fmt.Println("Commands: create, add, check, info")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "create":
		createCmd.Parse(os.Args[2:])
		doCreate(*createCapacity, *createFPP, *createOutput)
	case "add":
		addCmd.Parse(os.Args[2:])
		output := *addOutput
		if output == "" {
			output = *addFilter
		}
		doAdd(*addFilter, *addInput, output)
	case "check":
		checkCmd.Parse(os.Args[2:])
		doCheck(*checkFilter, *checkInput)
	case "info":
		infoCmd.Parse(os.Args[2:])
		doInfo(*infoFilter)
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func doCreate(capacity uint, fpp float64, output string) {
	filter := bloom.Initialize(uint64(capacity), fpp)

	f, err := os.Create(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := filter.Write(f); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing filter: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created bloom filter: %s\n", output)
	fmt.Printf("Capacity: %d\n", capacity)
	fmt.Printf("FPP: %f\n", fpp)
}

func loadFilter(path string) *bloom.BloomFilter {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	filter, err := bloom.Read(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading filter: %v\n", err)
		os.Exit(1)
	}

	return filter
}

func saveFilter(filter *bloom.BloomFilter, path string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := filter.Write(f); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing filter: %v\n", err)
		os.Exit(1)
	}
}

func doAdd(filterPath, inputPath, outputPath string) {
	filter := loadFilter(filterPath)

	var scanner *bufio.Scanner
	if inputPath == "" {
		scanner = bufio.NewScanner(os.Stdin)
	} else {
		f, err := os.Open(inputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		scanner = bufio.NewScanner(f)
	}

	// Increase buffer size for long lines
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	count := 0
	for scanner.Scan() {
		filter.Add(scanner.Bytes())
		count++
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	saveFilter(filter, outputPath)
	fmt.Printf("Added %d items\n", count)
}

func doCheck(filterPath, inputPath string) {
	filter := loadFilter(filterPath)

	var scanner *bufio.Scanner
	if inputPath == "" {
		scanner = bufio.NewScanner(os.Stdin)
	} else {
		f, err := os.Open(inputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		scanner = bufio.NewScanner(f)
	}

	for scanner.Scan() {
		if filter.Check(scanner.Bytes()) {
			fmt.Println("1")
		} else {
			fmt.Println("0")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

func doInfo(filterPath string) {
	filter := loadFilter(filterPath)

	// Get file size
	info, err := os.Stat(filterPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting file info: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Filter: %s\n", filterPath)
	fmt.Printf("Size (bytes): %d\n", info.Size())
	fmt.Printf("Number of bits: %d\n", filter.M)
	fmt.Printf("Hash functions: %d\n", filter.K)
	fmt.Printf("Capacity: %d\n", filter.N)
	fmt.Printf("Target FPP: %f\n", filter.P)
}
