package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/kargakis/sorter/pkg/store"
)

var (
	byName     = flag.Bool("name", false, "If set to true, sort input file by name")
	byAddress  = flag.Bool("address", false, "If set to true, sort input file by address")
	bufferSize = flag.Int("buffer-size", 18, "Buffer size")
	input      = flag.String("input", "", "Input file")
	output     = flag.String("output", "", "Output file")
)

func sortInMemory(lines []*store.Line, byName bool, byAddress bool) {
	switch {
	case byName:
		sort.Sort(store.ByName(lines))

	case byAddress:
		sort.Sort(store.ByAddress(lines))
	}
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
	if *byAddress && *byName {
		fmt.Fprintln(os.Stderr, "Cannot use both -name and -address, choose one of the two")
		os.Exit(1)
	}

	if !*byAddress && !*byName {
		fmt.Fprintln(os.Stderr, "Need to sort based on -name or -address")
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
	s := store.NewStore(*byAddress, *byName)
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
}
