package bucketsort_test

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kargakis/sortondisk/pkg/bucketsort"
)

func getLinesForFile(t *testing.T, path string) []*bucketsort.Line {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	content := make([]*bucketsort.Line, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line bucketsort.Line
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("Failed to unmarshal %s: %v\n", scanner.Text(), err)
		}
		content = append(content, &line)
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
	s := bucketsort.New(18, true, false, gotOut.Name())
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

	t.Log("Checking loc against original input")
	inputContent := getLinesForFile(t, "../../test/data.in")
	if len(gotOutContent) != len(inputContent) {
		t.Fatalf("expected output file length %d, got %d (%s)", len(inputContent), len(gotOutContent), gotOut.Name())
	}

	// Ensure input content matches output content in loc and values
	t.Log("Checking against original input content")
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
	s := bucketsort.New(25, false, true, gotOut.Name())
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

	t.Log("Checking loc against original input")
	inputContent := getLinesForFile(t, "../../test/data.in")
	if len(gotOutContent) != len(inputContent) {
		t.Fatalf("expected output file length %d, got %d (%s)", len(inputContent), len(gotOutContent), gotOut.Name())
	}

	// Ensure input content matches output content in loc and values
	t.Log("Checking against original input content")
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
