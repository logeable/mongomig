package backup

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func TestTenantHourRangeFilter(t *testing.T) {
	start := time.Date(2026, 5, 18, 7, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	f, err := TenantHourRangeFilter("tenant_key", "t1", false, "created_at", start, end)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bson.MarshalExtJSON(f, false, false)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if !strings.Contains(s, "tenant_key") || !strings.Contains(s, "t1") {
		t.Fatalf("%s", s)
	}
}
