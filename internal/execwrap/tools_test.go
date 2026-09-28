package execwrap

import (
	"reflect"
	"testing"
)

func TestRestoreNamespaceArgsDisablesMetadataIndexes(t *testing.T) {
	got := restoreNamespaceArgs("mongodb://localhost", "revol", "orders", true, false)
	want := []string{
		"--uri", "mongodb://localhost",
		"--db", "revol",
		"--collection", "orders",
		"--gzip",
		"--noIndexRestore",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestRestoreNamespaceArgsDrop(t *testing.T) {
	got := restoreNamespaceArgs("mongodb://localhost", "revol", "orders", false, true)
	if len(got) == 0 || got[len(got)-1] != "--drop" {
		t.Fatalf("expected --drop at end, got %#v", got)
	}
}
