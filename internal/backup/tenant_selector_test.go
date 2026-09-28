package backup

import (
	"os"
	"path/filepath"
	"testing"
)

const testTenantKeyA = "isolation_1eff7eac34924bd390ead2f9431f04bd"
const testTenantKeyB = "isolation_2eff7eac34924bd390ead2f9431f04bd"

func TestResolveRestoreTenantKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tenants.txt")
	if err := os.WriteFile(path, []byte("# comment\n"+testTenantKeyB+"\n\n"+testTenantKeyA+"\n"+testTenantKeyB+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := ResolveRestoreTenantKeys([]string{testTenantKeyA}, path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{testTenantKeyA, testTenantKeyB}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestResolveRestoreTenantKeysRejectsInvalidLength(t *testing.T) {
	if _, err := ResolveRestoreTenantKeys([]string{"tenant"}, ""); err == nil {
		t.Fatal("expected invalid tenant key error")
	}
}
