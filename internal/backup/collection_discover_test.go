package backup

import (
	"testing"
)

func TestResolveCollectionSpecs_explicitNames(t *testing.T) {
	specs, err := ResolveCollectionSpecs(t.Context(), nil, "", "revol", "a,b")
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 || specs[0].DB != "revol" || specs[0].Coll != "a" {
		t.Fatalf("%+v", specs)
	}
}

func TestResolveCollectionSpecs_dbDotColl(t *testing.T) {
	specs, err := ResolveCollectionSpecs(t.Context(), nil, "", "revol", "other.c1")
	if err != nil {
		t.Fatal(err)
	}
	if specs[0].DB != "other" || specs[0].Coll != "c1" {
		t.Fatalf("%+v", specs)
	}
}

func TestResolveCollectionSpecs_requiresDB(t *testing.T) {
	_, err := ResolveCollectionSpecs(t.Context(), nil, "", "", "")
	if err == nil {
		t.Fatal("expected error without db")
	}
}
