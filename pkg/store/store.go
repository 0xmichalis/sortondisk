package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

type Line struct {
	Name    string
	Address string
}

type ByName []*Line

func (b ByName) Len() int      { return len(b) }
func (b ByName) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b ByName) Less(i, j int) bool {
	return b[i].Name < b[j].Name
}

type ByAddress []*Line

func (b ByAddress) Len() int      { return len(b) }
func (b ByAddress) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b ByAddress) Less(i, j int) bool {
	return b[i].Address < b[j].Address
}

type Store struct {
	// TODO: Could also use an internal buffer of bufferSize
	// to batch disk writes
	bufferSize     int
	byAddress      bool
	byName         bool
	outputFilePath string

	// temp maps keys to temporary buckets where the input file
	// is chunked into
	temp map[string]*os.File
	// tempSize maps keys to the bucket size
	tempSize map[string]int
}

func NewStore(bufferSize int, byAddress bool, byName bool, outputFilePath string) *Store {
	return &Store{
		bufferSize:     bufferSize,
		byAddress:      byAddress,
		byName:         byName,
		outputFilePath: outputFilePath,
		temp:           make(map[string]*os.File),
		tempSize:       make(map[string]int),
	}
}

func (s *Store) CreateBucketsForFile(file *os.File, keySize int) error {
	scanner := bufio.NewScanner(file)
	createdKeys := make([]string, 0)

	for scanner.Scan() {
		var line Line
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			return fmt.Errorf("Failed to unmarshal %s: %w\n", scanner.Text(), err)
		}
		if key, err := s.Add(&line, keySize); err != nil {
			return fmt.Errorf("Failed to store %v: %w\n", line, err)
		} else {
			createdKeys = append(createdKeys, key)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Got scanner error: %v\n", err)
	}

	// Iterate over all newly created buckets and chunk further if necessary
	for _, key := range createdKeys {
		nextFile := s.temp[key]
		bucketSize := s.tempSize[key]

		if s.bufferSize < bucketSize {
			if err := s.CreateBucketsForFile(nextFile, keySize+1); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Store) Add(line *Line, keySize int) (string, error) {
	var err error
	var key string

	// Use key to chunk input file into buckets
	if s.byAddress {
		key = line.Address
	} else {
		key = line.Name
	}
	key, err = getKey(line, keySize, s.byAddress, s.byName)
	if err != nil {
		return "", err
	}

	file, ok := s.temp[key]
	if !ok {
		file, err = ioutil.TempFile("", key)
		if err != nil {
			return key, err
		}
		s.temp[key] = file
	} else {
		file, err = os.OpenFile(file.Name(), os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return key, err
		}
	}
	// defer file.Close()

	lineToStore, err := json.Marshal(line)
	if err != nil {
		return key, err
	}
	fmt.Printf("[Add] Writting %v in %s\n", string(lineToStore), file.Name())
	if _, err = file.WriteString(string(lineToStore) + "\n"); err != nil {
		return key, err
	}

	// Keep track of each bucket size so the algorithm can be informed easily
	// whether it needs to continue chunking buckets or whether it can sort
	// in memory based on the provided buffer size.
	s.tempSize[key]++

	return key, nil
}

func getKey(line *Line, keySize int, byAddress bool, byName bool) (string, error) {
	switch {
	case byName:
		if keySize >= len(line.Name) {
			return line.Name, nil
		}
		return line.Name[:keySize], nil

	case byAddress:
		if keySize >= len(line.Address) {
			return line.Address, nil
		}
		return line.Address[:keySize], nil

	default:
		return "", errors.New("expected to choose address or name")
	}
}

func sortLines(lines []*Line, byAddress bool, byName bool) {
	switch {
	case byAddress:
		sort.Sort(ByAddress(lines))

	case byName:
		sort.Sort(ByName(lines))
	}
}

func (s *Store) cleanup() {
	for _, file := range s.temp {
		if err := os.Remove(file.Name()); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to remove temp file %s: %v", file.Name(), err)
		}
	}
}

func (s *Store) Sort() error {
	defer s.cleanup()
	keys := make([]string, 0)

	for key := range s.temp {
		fileSize := s.tempSize[key]
		if s.bufferSize >= fileSize {
			keys = append(keys, key)
		}
	}

	outputFile, err := os.Create(s.outputFilePath)
	if err != nil {
		return err
	}

	sort.Strings(keys)
	for _, key := range keys {
		lines := make([]*Line, 0)
		bucket := s.temp[key]
		bucket, err = os.OpenFile(bucket.Name(), os.O_RDONLY, 0777)
		if err != nil {
			return err
		}
		defer bucket.Close()

		fmt.Printf("[Sort] Sorting bucket %s (%s)\n", key, bucket.Name())
		// sort if possible, otherwise break down to
		// smaller files
		// TODO: Size check
		scanner := bufio.NewScanner(bucket)
		for scanner.Scan() {
			var line Line
			if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to unmarshal %s: %v\n", scanner.Text(), err)
				continue
			}
			fmt.Printf("[Sort] Sorting line %s %s\n", line.Address, line.Name)
			lines = append(lines, &line)
		}

		sortLines(lines, s.byAddress, s.byName)
		for _, line := range lines {
			lineToStore, err := json.Marshal(line)
			if err != nil {
				return err
			}
			fmt.Printf("[Sort] Writting line %s %s\n", line.Address, line.Name)
			outputFile.Write(lineToStore)
			outputFile.Write([]byte("\n"))
		}
	}

	return nil
}
