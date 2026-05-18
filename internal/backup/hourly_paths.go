package backup

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// HourBucket identifies a UTC hour partition.
type HourBucket struct {
	Year  int
	Month int
	Day   int
	Hour  int
	Start time.Time // UTC inclusive
	End   time.Time // UTC exclusive
}

// HourBucketUTC builds a bucket from UTC wall time (minute/second zeroed).
func HourBucketUTC(t time.Time) HourBucket {
	u := t.UTC()
	start := time.Date(u.Year(), u.Month(), u.Day(), u.Hour(), 0, 0, 0, time.UTC)
	return HourBucket{
		Year:  start.Year(),
		Month: int(start.Month()),
		Day:   start.Day(),
		Hour:  start.Hour(),
		Start: start,
		End:   start.Add(time.Hour),
	}
}

// ParseHourFlag parses YYYY-MM-DDTHH in UTC.
func ParseHourFlag(s string) (HourBucket, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return HourBucket{}, fmt.Errorf("hour flag is empty")
	}
	if len(s) == len("2006-01-02T15") {
		t, err := time.ParseInLocation("2006-01-02T15", s, time.UTC)
		if err != nil {
			return HourBucket{}, fmt.Errorf("hour %q: use YYYY-MM-DDTHH (UTC)", s)
		}
		return HourBucketUTC(t), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return HourBucket{}, fmt.Errorf("hour %q: use YYYY-MM-DDTHH or RFC3339 (UTC)", s)
	}
	return HourBucketUTC(t), nil
}

func (b HourBucket) String() string {
	return fmt.Sprintf("%04d-%02d-%02dT%02dZ", b.Year, b.Month, b.Day, b.Hour)
}

func (b HourBucket) PathSegment() string {
	return filepath.Join(
		strconv.Itoa(b.Year),
		fmt.Sprintf("%02d", b.Month),
		fmt.Sprintf("%02d", b.Day),
		fmt.Sprintf("%02d", b.Hour),
	)
}

// TenantShard returns the first-level path segment (2 chars after last '_').
func TenantShard(tenantKey string) (string, error) {
	tenantKey = strings.TrimSpace(tenantKey)
	if tenantKey == "" {
		return "", fmt.Errorf("tenant_key is empty")
	}
	if strings.ContainsAny(tenantKey, "/\\") {
		return "", fmt.Errorf("tenant_key %q contains path separator", tenantKey)
	}
	_, suffix, ok := strings.Cut(tenantKey, "_")
	if !ok {
		return "", fmt.Errorf("tenant_key %q: missing '_' for shard prefix", tenantKey)
	}
	if len(suffix) < 2 {
		return "", fmt.Errorf("tenant_key %q: suffix too short for shard", tenantKey)
	}
	return suffix[:2], nil
}

// TenantDataRelPath is relative to the hour prefix.
func TenantDataRelPath(shard, tenantKey string) string {
	return filepath.ToSlash(filepath.Join(shard, tenantKey, "dump.tar"))
}

// CollectionBase returns {remotePrefix}/{db}/{collection} without trailing slash.
func CollectionBase(remotePrefix, db, coll string) string {
	return filepath.ToSlash(filepath.Join(strings.Trim(remotePrefix, "/"), db, coll))
}

func HourBase(collectionBase string, b HourBucket) string {
	return collectionBase + "/" + b.PathSegment()
}

func CollectionMetaKey(collectionBase string) string {
	return collectionBase + "/meta.json"
}

func CollectionIndexesKey(collectionBase string) string {
	return collectionBase + "/indexes.json"
}

func HourMetaKey(hourBase string) string {
	return hourBase + "/meta.json"
}

func TenantObjectKey(hourBase, dataRelPath string) string {
	return hourBase + "/" + dataRelPath
}

// NextHour returns the following UTC hour bucket.
func (b HourBucket) Next() HourBucket {
	return HourBucketUTC(b.End)
}

// HoursInclusive returns each UTC hour bucket from start through end inclusive.
func HoursInclusive(start, end HourBucket) []HourBucket {
	if end.Start.Before(start.Start) {
		return nil
	}
	var out []HourBucket
	for cur := start; !cur.Start.After(end.Start); cur = cur.Next() {
		out = append(out, cur)
	}
	return out
}
