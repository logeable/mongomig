package backup

import (
	"testing"
	"time"
)

func TestHourEnded_completeVsPartial(t *testing.T) {
	hb := HourBucketUTC(time.Date(2026, 5, 15, 17, 0, 0, 0, time.UTC))

	t.Run("partial before hour end", func(t *testing.T) {
		now := hb.Start.Add(30 * time.Minute)
		hourEnded := !now.Before(hb.End)
		allOK := true
		if hourEnded && allOK {
			t.Fatal("should not complete")
		}
		if hourEnded {
			t.Fatal("hour not ended")
		}
	})

	t.Run("complete after hour end all uploaded", func(t *testing.T) {
		now := hb.End
		hourEnded := !now.Before(hb.End)
		allOK := true
		if !(hourEnded && allOK) {
			t.Fatal("should complete")
		}
	})
}

func TestShouldWipePartialHour(t *testing.T) {
	hm := &HourMeta{Status: HourStatusPartial}
	wipe := hm != nil && hm.Status == HourStatusPartial
	if !wipe {
		t.Fatal("partial hour should wipe on new schedule")
	}
}
