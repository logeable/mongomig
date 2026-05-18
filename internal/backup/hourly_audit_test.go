package backup

import "testing"

func TestParseHourMetaObjectKey(t *testing.T) {
	base := "mongomig/revol/users"
	b, ok := ParseHourMetaObjectKey(base, base+"/2026/05/18/07/meta.json")
	if !ok {
		t.Fatal("expected ok")
	}
	if b.Year != 2026 || b.Month != 5 || b.Day != 18 || b.Hour != 7 {
		t.Fatalf("bucket: %+v", b)
	}
	if _, ok := ParseHourMetaObjectKey(base, base+"/meta.json"); ok {
		t.Fatal("collection meta should not parse as hour")
	}
	if _, ok := ParseHourMetaObjectKey(base, base+"/indexes.json"); ok {
		t.Fatal("indexes should not parse as hour")
	}
}

func TestFixHintForHour_partial(t *testing.T) {
	row := HourAuditRow{
		Bucket:      HourBucket{Year: 2026, Month: 5, Day: 18, Hour: 7},
		Status:      HourStatusPartial,
		StatusValid: true,
		Uploaded:    2,
		Total:       5,
	}
	row.FixHint = fixHintForHour(row)
	if row.FixHint == "" {
		t.Fatal("expected fix hint")
	}
}
