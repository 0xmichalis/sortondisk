package sort_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kargakis/sorter/pkg/sort"
)

func getLinesForFile(file *os.File) ([]*sort.Line, error) {
	content := make([]*sort.Line, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line sort.Line
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			return nil, fmt.Errorf("Failed to unmarshal %s: %v\n", scanner.Text(), err)
		}
		content = append(content, &line)
	}

	return content, nil
}

func sliceToMap(lines []*sort.Line) map[string]string {
	content := make(map[string]string)
	for _, line := range lines {
		content[line.Name] = line.Address
	}
	return content
}

func TestSortByAddress(t *testing.T) {
	expectedOut, err := os.Open("../../test/data_address")
	if err != nil {
		t.Fatal(err)
	}
	defer expectedOut.Close()
	expectedOutContent, err := getLinesForFile(expectedOut)
	if err != nil {
		t.Fatal(err)
	}

	input, err := os.Open("../../test/data.in")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	inputContent, err := getLinesForFile(input)
	if err != nil {
		t.Fatal(err)
	}

	gotOut, err := ioutil.TempFile("", "sort_by_address_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(gotOut.Name())

	s := sort.New(25, true, false, gotOut.Name())
	if err := s.Sort(input); err != nil {
		t.Fatal(err)
	}
	gotOutContent, err := getLinesForFile(gotOut)
	if err != nil {
		t.Fatal(err)
	}

	if len(gotOutContent) != len(expectedOutContent) {
		t.Fatalf("expected output file length %d, got %d", len(expectedOutContent), len(gotOutContent))
	}

	// Compare expectedOut with gotOut content
	for i, line := range expectedOutContent {
		gotLine := gotOutContent[i]
		if line.Name != gotLine.Name {
			t.Fatalf("expected name %s, got %s", line.Name, gotLine.Name)
		}
		if line.Address != gotLine.Address {
			t.Fatalf("expected address %s, got %s", line.Address, gotLine.Address)
		}
	}

	// Ensure input content matches output content in loc and values
	gotOutMap := sliceToMap(gotOutContent)
	for name, address := range sliceToMap(inputContent) {
		gotAddress, ok := gotOutMap[name]
		if !ok {
			t.Fatalf("missing name %s in original input", name)
		}
		if address != gotAddress {
			t.Fatalf("expected address %s, got %s", address, gotAddress)
		}
	}
}

func TestSortByName(t *testing.T) {
	expectedOut, err := os.Open("../../test/data_address")
	if err != nil {
		t.Fatal(err)
	}
	defer expectedOut.Close()

	input, err := os.Open("../../test/data.in")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	gotOut, err := ioutil.TempFile("", "sort_by_name_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(gotOut.Name())

	s := sort.New(25, false, true, gotOut.Name())
	if err := s.Sort(input); err != nil {
		t.Fatal(err)
	}

	// TODO: Compare expectedOut with gotOut content
	// TODO: Ensure input loc matches output loc
}
