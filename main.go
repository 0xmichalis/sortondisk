package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/kargakis/sorter/pkg/sort"
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

	s := sort.New(*bufferSize, *byAddress, *byName, *output)
	if err := s.Sort(file); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to sort: %v\n", err)
		os.Exit(1)
	}
}
