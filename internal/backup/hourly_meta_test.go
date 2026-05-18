package backup

import "testing"

func TestTenantObjectKeyFromDataRelPath(t *testing.T) {
	hourBase := "mongomig/revol/c1/2026/05/18/07"
	rel := "1e/isolation_abc/dump.tar"
	want := hourBase + "/" + rel
	if got := TenantObjectKey(hourBase, rel); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestValidateHourStatus(t *testing.T) {
	if err := validateHourStatus(HourStatusPartial, "test"); err != nil {
		t.Fatal(err)
	}
	if err := validateHourStatus(HourStatusComplete, "test"); err != nil {
		t.Fatal(err)
	}
	if err := validateHourStatus(HourStatus("in_progress"), "test"); err == nil {
		t.Fatal("expected error for in_progress")
	}
}
