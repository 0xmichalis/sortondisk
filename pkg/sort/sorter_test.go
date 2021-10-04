package sort_test

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kargakis/sorter/pkg/sort"
)

func getLinesForFile(t *testing.T, path string) []*sort.Line {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	content := make([]*sort.Line, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line sort.Line
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("Failed to unmarshal %s: %v\n", scanner.Text(), err)
		}
		content = append(content, &line)
	}

	return content
}

func sliceToMap(lines []*sort.Line) map[string]string {
	content := make(map[string]string)
	for _, line := range lines {
		content[line.Name] = line.Address
	}
	return content
}

func TestSortByAddress(t *testing.T) {
	// Prepare inputs
	input, err := os.Open("../../test/data.in")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	gotOut, err := ioutil.TempFile("", "sort_by_address_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(gotOut.Name())

	// Run sorter
	s := sort.New(25, true, false, gotOut.Name())
	if err := s.Sort(input); err != nil {
		t.Fatal(err)
	}
	t.Logf("Sorted file %s", gotOut.Name())

	// Compare outputs
	expectedOutContent := getLinesForFile(t, "../../test/data_address")
	gotOutContent := getLinesForFile(t, gotOut.Name())

	t.Log("Checking expected loc")
	if len(gotOutContent) != len(expectedOutContent) {
		t.Fatalf("expected output file length %d, got %d (%s)", len(expectedOutContent), len(gotOutContent), gotOut.Name())
	}

	// Compare expectedOut with gotOut content
	t.Log("Checking expected content")
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
	t.Log("Checking against original input content")
	inputContent := getLinesForFile(t, "../../test/data.in")
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
	// Prepare inputs
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

	// Run sorter
	s := sort.New(25, false, true, gotOut.Name())
	if err := s.Sort(input); err != nil {
		t.Fatal(err)
	}
	t.Logf("Sorted file %s", gotOut.Name())

	// Compare outputs
	expectedOutContent := getLinesForFile(t, "../../test/data_name")
	gotOutContent := getLinesForFile(t, gotOut.Name())

	t.Log("Checking expected loc")
	if len(gotOutContent) != len(expectedOutContent) {
		t.Fatalf("expected output file length %d, got %d (%s)", len(expectedOutContent), len(gotOutContent), gotOut.Name())
	}

	// Compare expectedOut with gotOut content
	t.Log("Checking expected content")
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
	t.Log("Checking against original input content")
	inputContent := getLinesForFile(t, "../../test/data.in")
	for _, inputLine := range inputContent {
		lineExists := false
		for _, outputLine := range gotOutContent {
			if inputLine.Address == outputLine.Address &&
				inputLine.Name == outputLine.Name {
				lineExists = true
			}
		}
		if !lineExists {
			t.Fatalf("Cannot find line with name=%s and address=%s in output", inputLine.Name, inputLine.Address)
		}
	}
}
