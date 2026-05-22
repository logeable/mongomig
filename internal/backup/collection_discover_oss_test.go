package backup

import "testing"

func TestCollectionNamesFromOSSKeys(t *testing.T) {
	prefix := "mongomig/revol/"
	keys := []string{
		"mongomig/revol/coll_a/meta.json",
		"mongomig/revol/coll_a/2026/05/18/07/meta.json",
		"mongomig/revol/coll_b/meta.json",
		"mongomig/revol/_mongomig_restore/meta.json",
		"mongomig/other/meta.json",
	}
	got := collectionNamesFromOSSKeys(prefix, keys)
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	want := map[string]bool{"coll_a": true, "coll_b": true, "_mongomig_restore": true}
	for _, c := range got {
		if !want[c] {
			t.Fatalf("unexpected %q in %v", c, got)
		}
		delete(want, c)
	}
	if len(want) != 0 {
		t.Fatalf("missing: %v got %v", want, got)
	}
}

func TestCollectionMetaOnOSS(t *testing.T) {
	base := "mongomig/revol/samples"
	keys := []string{base + "/meta.json", base + "/2026/05/18/00/meta.json"}
	if !collectionMetaOnOSS(keys, base) {
		t.Fatal("expected meta present")
	}
	if collectionMetaOnOSS(keys, "mongomig/revol/missing") {
		t.Fatal("expected missing")
	}
}
