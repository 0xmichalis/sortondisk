package store

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
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
	temp      map[string]*os.File
	byAddress bool
	byName    bool
}

func NewStore(byAddress bool, byName bool) *Store {
	return &Store{
		temp:      make(map[string]*os.File),
		byAddress: byAddress,
		byName:    byName,
	}
}

func (s *Store) Add(line *Line) error {
	var err error
	var key string
	if s.byAddress {
		key = line.Address
	} else {
		key = line.Name
	}
	key, err = getKey(line, 2, s.byAddress, s.byName)
	if err != nil {
		return err
	}

	file, ok := s.temp[key]
	if !ok {
		file, err = ioutil.TempFile("", key)
		if err != nil {
			return err
		}
		s.temp[key] = file
	}
	lineToStore, err := json.Marshal(line)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file.Name(), lineToStore, 0777)
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
