package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/kargakis/sorter/pkg/store"
)

var (
	byName     = flag.Bool("name", false, "If set to true, sort input file by name")
	byAddress  = flag.Bool("address", false, "If set to true, sort input file by address")
	bufferSize = flag.Int("buffer-size", 18, "Buffer size")
	input      = flag.String("input", "", "Input file")
	output     = flag.String("output", "", "Output file")
)

func validateFlags() error {
	if *byAddress && *byName {
		return errors.New("Cannot use both -name and -address, choose one of the two")
	}

	if !*byAddress && !*byName {
		return errors.New("Need to sort based on -name or -address")
	}

	if *input == "" {
		return errors.New("Need to provide an input file via -input")
	}

	if *output == "" {
		return errors.New("Need to provide an output file via -output")
	}

	return nil
}

// TODO: First input file iteration can be executed
// at the same time as chunking the file into smaller
// pieces that can fit bufferSize.
func getLoc(inputFile *os.File) int {
	var loc int
	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan() {
		loc++
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	return loc
}

func main() {
	flag.Parse()

	// Input validation
	if err := validateFlags(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Input file read
	file, err := os.Open(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open input file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Get lines of code to determine whether we can sort in memory
	// or need to chunk the file on the disk before sorting
	// loc := getLoc(file)
	// _ = loc
	// if *bufferSize >= loc {
	// 	sortInMemory()
	// }

	scanner := bufio.NewScanner(file)
	s := store.NewStore(*byAddress, *byName, *output)
	for scanner.Scan() {
		var line store.Line
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to unmarshal %s: %v\n", scanner.Text(), err)
			continue
		}
		if err := s.Add(&line); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to store %v: %v\n", line, err)
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	if err := s.Sort(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to sort: %v\n", err)
	}
}
