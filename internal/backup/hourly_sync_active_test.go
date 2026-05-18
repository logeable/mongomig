package backup

import (
	"testing"
	"time"
)

func TestWarnCompletedGap_detectsSkippedHour(t *testing.T) {
	nc := HourRefFromBucket(HourBucketUTC(time.Date(2026, 5, 18, 5, 0, 0, 0, time.UTC)), "2026/05/18/05/meta.json", HourStatusComplete)
	active := HourRefFromBucket(HourBucketUTC(time.Date(2026, 5, 18, 7, 0, 0, 0, time.UTC)), "2026/05/18/07/meta.json", HourStatusPartial)
	cm := &CollectionMeta{NewestCompleted: &nc, Active: &active}
	next := cm.NewestCompleted.Bucket().Next()
	activeB := cm.Active.Bucket()
	if !activeB.Start.After(next.Start) || sameHour(activeB, next) {
		t.Fatalf("fixture should represent gap at hour 6: next=%s active=%s", next.String(), activeB.String())
	}
}

func TestSameHour(t *testing.T) {
	a := HourBucketUTC(time.Date(2026, 5, 18, 6, 0, 0, 0, time.UTC))
	b := HourBucketUTC(time.Date(2026, 5, 18, 6, 30, 0, 0, time.UTC))
	if !sameHour(a, b) {
		t.Fatal("same UTC hour should match")
	}
}
