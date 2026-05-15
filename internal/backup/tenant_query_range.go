package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// WriteTenantDayRangeQueryFile writes a mongodump --queryFile JSON filter:
// { "$and": [ {tenantField: tenantKey}, {timeField: {$gte,$lt on $date}} ] }.
func WriteTenantDayRangeQueryFile(path, tenantField, tenantKey string, tenantNumeric bool, timeField string, startUTC, endUTC time.Time) error {
	if err := ValidateBSONFieldName(tenantField); err != nil {
		return fmt.Errorf("tenant field: %w", err)
	}
	if err := ValidateBSONFieldName(timeField); err != nil {
		return fmt.Errorf("time field: %w", err)
	}
	startS := startUTC.UTC().Format("2006-01-02T15:04:05.000Z")
	endS := endUTC.UTC().Format("2006-01-02T15:04:05.000Z")

	var tenantClause []byte
	var err error
	if tenantNumeric {
		tenantClause = []byte(fmt.Sprintf(`{"%s":%s}`, tenantField, tenantKey))
	} else {
		tenantClause, err = json.Marshal(map[string]string{tenantField: tenantKey})
		if err != nil {
			return err
		}
	}
	timeClause, err := json.Marshal(map[string]any{
		timeField: map[string]any{
			"$gte": map[string]string{"$date": startS},
			"$lt":  map[string]string{"$date": endS},
		},
	})
	if err != nil {
		return err
	}
	out, err := json.Marshal(map[string]any{
		"$and": []json.RawMessage{
			json.RawMessage(tenantClause),
			json.RawMessage(timeClause),
		},
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o600)
}
