package main

import (
	"bufio"
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

	const initialKeySize = 2
	s := store.NewStore(*bufferSize, *byAddress, *byName, *output)

	if err := s.CreateBucketsForFile(file, initialKeySize); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to bucket input file: %v\n", err)
		os.Exit(1)
	}

	if err := s.Sort(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to sort: %v\n", err)
		os.Exit(1)
	}
}
