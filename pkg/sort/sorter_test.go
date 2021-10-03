package sort_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/kargakis/sorter/pkg/sort"
)

func TestSortByAddress(t *testing.T) {
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

	gotOut, err := ioutil.TempFile("", "sort_by_address_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(gotOut.Name())

	s := sort.New(25, true, false, gotOut.Name())
	if err := s.Sort(input); err != nil {
		t.Fatal(err)
	}

	// TODO: Compare expectedOut with gotOut content
	// TODO: Ensure input loc matches output loc
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
