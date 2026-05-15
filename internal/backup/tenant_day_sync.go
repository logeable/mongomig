package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const tenantDaySyncSchema = 1

// TenantDaySyncState is persisted on OSS under _meta/day-sync-<job>.json for resume and auditing.
type TenantDaySyncState struct {
	Schema        int                `json:"schema"`
	SyncJobID     string             `json:"sync_job_id"`
	TenantKey     string             `json:"tenant_key"`
	TenantField   string             `json:"tenant_field"`
	TenantNumeric bool               `json:"tenant_key_numeric"`
	TimeField     string             `json:"time_field"`
	Timezone      string             `json:"timezone"`
	Collections   []string           `json:"collections"`
	UpdatedAt     time.Time          `json:"updated_at"`
	DBMinDay      string             `json:"db_min_day,omitempty"`
	DBMaxDay      string             `json:"db_max_day,omitempty"`
	RangeStartDay string             `json:"range_start_day,omitempty"`
	RangeEndDay   string             `json:"range_end_day,omitempty"`
	Days          []TenantDaySyncRow `json:"days"`
}

// TenantDaySyncRow records one civil-day export attempt.
type TenantDaySyncRow struct {
	CivilDay             string    `json:"civil_day"`
	Status               string    `json:"status"` // ok | failed | skipped
	PartitionPathSegment string    `json:"partition_path_segment,omitempty"`
	BackupID             string    `json:"backup_id,omitempty"`
	Error                string    `json:"error,omitempty"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// TenantDaySyncOptions configures oss export-days.
type TenantDaySyncOptions struct {
	TenantKey     string
	Collections   []NSSpec
	TenantField   string
	TenantNumeric bool
	TimeField     string
	Timezone      string
	FromDay       string
	ToDay         string
	SyncJobID     string
	CleanupLocal  bool
	DryRun        bool
	RedoFailed    bool
	SkipOKDays    bool
}

func isS3Missing(err error) bool {
	if err == nil {
		return false
	}
	var nk *types.NoSuchKey
	if errors.As(err, &nk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

func loadTenantDaySyncState(ctx context.Context, remote interface {
	DownloadFile(ctx context.Context, key, localPath string) error
}, key, tmpDir string) (*TenantDaySyncState, error) {
	local := filepath.Join(tmpDir, "day-sync-download.json")
	if err := remote.DownloadFile(ctx, key, local); err != nil {
		if isS3Missing(err) {
			return nil, nil
		}
		return nil, err
	}
	data, err := os.ReadFile(local)
	if err != nil {
		return nil, err
	}
	var st TenantDaySyncState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("parse day-sync state: %w", err)
	}
	if st.Schema != 0 && st.Schema != tenantDaySyncSchema {
		return nil, fmt.Errorf("unsupported day-sync schema %d", st.Schema)
	}
	return &st, nil
}

func saveTenantDaySyncState(ctx context.Context, remote interface {
	UploadFile(ctx context.Context, localPath, key string) error
}, key string, st *TenantDaySyncState, tmpDir string) error {
	st.Schema = tenantDaySyncSchema
	st.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	local := filepath.Join(tmpDir, "day-sync-upload.json")
	if err := os.WriteFile(local, data, 0o640); err != nil {
		return err
	}
	return remote.UploadFile(ctx, local, key)
}

func dayRowIndex(rows []TenantDaySyncRow, civil string) int {
	for i := range rows {
		if rows[i].CivilDay == civil {
			return i
		}
	}
	return -1
}

func upsertDayRow(rows []TenantDaySyncRow, row TenantDaySyncRow) []TenantDaySyncRow {
	if i := dayRowIndex(rows, row.CivilDay); i >= 0 {
		rows[i] = row
		return rows
	}
	return append(rows, row)
}

// RunTenantDayOSSSync walks civil days (in export timezone), exports each day to OSS, and updates _meta/day-sync state.
func (r *Runner) RunTenantDayOSSSync(ctx context.Context, opt TenantDaySyncOptions) error {
	if !r.cfg.S3Enabled() {
		return fmt.Errorf("OSS day sync requires S3 (--s3-endpoint and bucket)")
	}
	if err := r.cfg.EnsureStaging(); err != nil {
		return err
	}
	loc, err := LoadIANATimezone(opt.Timezone)
	if err != nil {
		return err
	}
	tf := strings.TrimSpace(opt.TimeField)
	if tf == "" {
		tf = "created_at"
	}
	if err := ValidateBSONFieldName(tf); err != nil {
		return err
	}
	tenantSeg, err := SanitizeTenantPath(opt.TenantKey)
	if err != nil {
		return err
	}
	jobID := strings.TrimSpace(opt.SyncJobID)
	if jobID == "" {
		jobID = "default"
	}
	jobSan, err := sanitizeSyncJobID(jobID)
	if err != nil {
		return err
	}
	progressKey, err := TenantDaySyncProgressKey(r.cfg, tenantSeg, jobSan)
	if err != nil {
		return err
	}

	metaDir := filepath.Join(r.cfg.StagingDir, "_meta_upload")
	if err := os.MkdirAll(metaDir, 0o750); err != nil {
		return err
	}

	st, err := loadTenantDaySyncState(ctx, r.remote, progressKey, metaDir)
	if err != nil {
		return err
	}
	if st == nil {
		tfStr := strings.TrimSpace(opt.TenantField)
		if tfStr == "" {
			tfStr = "tenant_key"
		}
		st = &TenantDaySyncState{
			Schema:        tenantDaySyncSchema,
			SyncJobID:     jobSan,
			TenantKey:     opt.TenantKey,
			TenantField:   tfStr,
			TenantNumeric: opt.TenantNumeric,
			TimeField:     tf,
			Timezone:      loc.String(),
			Collections:   collectionNames(opt.Collections),
		}
	} else {
		if st.TenantKey != opt.TenantKey {
			return fmt.Errorf("existing progress tenant_key %q != --tenant-key %q (use a different --sync-job-id or delete OSS key)", st.TenantKey, opt.TenantKey)
		}
		if st.SyncJobID != jobSan {
			return fmt.Errorf("existing progress sync_job_id %q != %q", st.SyncJobID, jobSan)
		}
		if strings.TrimSpace(st.TenantField) == "" {
			st.TenantField = "tenant_key"
		}
		if strings.TrimSpace(st.TimeField) != "" && st.TimeField != tf {
			return fmt.Errorf("existing progress time_field %q != --time-field %q", st.TimeField, tf)
		}
		if optField := strings.TrimSpace(opt.TenantField); optField != "" && st.TenantField != optField {
			return fmt.Errorf("existing progress tenant_field %q != --tenant-field %q", st.TenantField, optField)
		}
	}
	st.TimeField = tf
	st.Timezone = loc.String()
	st.TenantNumeric = opt.TenantNumeric
	st.Collections = collectionNames(opt.Collections)

	minUTC, maxUTC, hasBounds, err := GlobalTenantTimeBounds(ctx, r.cfg.MongoURI, opt.Collections, st.TenantField, opt.TenantKey, opt.TenantNumeric, st.TimeField)
	if err != nil {
		return err
	}
	if !hasBounds {
		return fmt.Errorf("no documents matched tenant filter on given collections (cannot infer date range)")
	}

	dbMinStart := TruncateToCivilMidnightInTZ(minUTC, loc)
	dbMaxStart := TruncateToCivilMidnightInTZ(maxUTC, loc)
	nowStart := TruncateToCivilMidnightInTZ(time.Now().UTC(), loc)

	st.DBMinDay = dbMinStart.Format("2006-01-02")
	st.DBMaxDay = dbMaxStart.Format("2006-01-02")

	startDay := dbMinStart
	if strings.TrimSpace(opt.FromDay) != "" {
		fd, err := CivilMidnightInTZ(opt.FromDay, loc)
		if err != nil {
			return err
		}
		startDay = maxTime(startDay, fd)
	}
	endDay := minTime(dbMaxStart, nowStart)
	if strings.TrimSpace(opt.ToDay) != "" {
		td, err := CivilMidnightInTZ(opt.ToDay, loc)
		if err != nil {
			return err
		}
		endDay = minTime(endDay, td)
	}
	if startDay.After(endDay) {
		return fmt.Errorf("effective day range empty: start %s end %s (from/to vs data bounds)", startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
	}
	st.RangeStartDay = startDay.Format("2006-01-02")
	st.RangeEndDay = endDay.Format("2006-01-02")

	days, err := CivilDaysInclusive(startDay, endDay, loc)
	if err != nil {
		return err
	}

	fmt.Printf("day-sync: tenant=%s job=%s tz=%s days=%d key=%s\n", opt.TenantKey, jobSan, loc.String(), len(days), progressKey)

	if !opt.DryRun {
		if err := saveTenantDaySyncState(ctx, r.remote, progressKey, st, metaDir); err != nil {
			return fmt.Errorf("save progress (range metadata): %w", err)
		}
	}

	for _, civil := range days {
		prev, ok := findDayRow(st.Days, civil)
		if opt.RedoFailed {
			if !ok || prev.Status != "failed" {
				continue
			}
		} else if opt.SkipOKDays && ok && prev.Status == "ok" {
			fmt.Printf("day-sync skip ok: %s\n", civil)
			continue
		}

		dayStart, err := CivilMidnightInTZ(civil, loc)
		if err != nil {
			return err
		}
		startUTC, endUTC, err := CivilDayRangeUTC(civil, loc)
		if err != nil {
			return err
		}
		partition := dayStart

		if opt.DryRun {
			fmt.Printf("day-sync dry-run: %s -> [%s, %s) UTC partition=%s\n", civil, startUTC.Format(time.RFC3339), endUTC.Format(time.RFC3339), PartitionPathSegment(partition))
			continue
		}

		m, err := r.RunTenantDaySnapshot(ctx, opt.TenantKey, partition, civil, loc.String(), opt.Collections, st.TenantField, opt.TenantNumeric, st.TimeField, startUTC, endUTC, opt.CleanupLocal)
		if err != nil {
			row := TenantDaySyncRow{
				CivilDay:  civil,
				Status:    "failed",
				Error:     err.Error(),
				UpdatedAt: time.Now().UTC(),
			}
			st.Days = upsertDayRow(st.Days, row)
			if saveErr := saveTenantDaySyncState(ctx, r.remote, progressKey, st, metaDir); saveErr != nil {
				return fmt.Errorf("day %s export failed: %v; additionally could not save progress: %w", civil, err, saveErr)
			}
			return fmt.Errorf("day %s: %w", civil, err)
		}
		row := TenantDaySyncRow{
			CivilDay:             civil,
			Status:               "ok",
			PartitionPathSegment: m.PartitionPathSegment,
			BackupID:             m.BackupID,
			UpdatedAt:            time.Now().UTC(),
		}
		st.Days = upsertDayRow(st.Days, row)
		if err := saveTenantDaySyncState(ctx, r.remote, progressKey, st, metaDir); err != nil {
			return fmt.Errorf("after ok export %s: save progress: %w", civil, err)
		}
		fmt.Printf("day-sync ok: %s backup_id=%s path_seg=%s\n", civil, m.BackupID, m.PartitionPathSegment)
	}
	return nil
}

func findDayRow(rows []TenantDaySyncRow, civil string) (TenantDaySyncRow, bool) {
	i := dayRowIndex(rows, civil)
	if i < 0 {
		return TenantDaySyncRow{}, false
	}
	return rows[i], true
}

func collectionNames(specs []NSSpec) []string {
	out := make([]string, 0, len(specs))
	for _, ns := range specs {
		out = append(out, ns.DB+"."+ns.Coll)
	}
	return out
}
