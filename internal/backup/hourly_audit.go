package backup

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/logeable/mongomig/internal/storage"
)

// HourAuditRow is one hour partition found on OSS.
type HourAuditRow struct {
	Bucket       HourBucket
	HourBase     string
	MetaKey      string
	Status       HourStatus
	StatusValid  bool
	Uploaded     int
	Total        int
	TenantErrors int
	FixHint      string
}

// CollectionAuditReport summarizes OSS meta for one collection.
type CollectionAuditReport struct {
	DB               string
	Coll             string
	CollectionBase   string
	CollectionFound  bool
	CollectionMeta   *CollectionMeta
	CollectionIssues []string
	Hours            []HourAuditRow
}

// ParseHourMetaObjectKey parses {collectionBase}/{y}/{m}/{d}/{h}/meta.json.
func ParseHourMetaObjectKey(collectionBase, key string) (HourBucket, bool) {
	collectionBase = strings.TrimSuffix(collectionBase, "/")
	key = strings.TrimPrefix(key, "/")
	if key == CollectionMetaKey(collectionBase) {
		return HourBucket{}, false
	}
	prefix := collectionBase + "/"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/meta.json") {
		return HourBucket{}, false
	}
	rel := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/meta.json")
	parts := strings.Split(rel, "/")
	if len(parts) != 4 {
		return HourBucket{}, false
	}
	y, err1 := strconv.Atoi(parts[0])
	mo, err2 := strconv.Atoi(parts[1])
	d, err3 := strconv.Atoi(parts[2])
	h, err4 := strconv.Atoi(parts[3])
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return HourBucket{}, false
	}
	if mo < 1 || mo > 12 || d < 1 || d > 31 || h < 0 || h > 23 {
		return HourBucket{}, false
	}
	start := time.Date(y, time.Month(mo), d, h, 0, 0, 0, time.UTC)
	return HourBucket{
		Year:  y,
		Month: mo,
		Day:   d,
		Hour:  h,
		Start: start,
		End:   start.Add(time.Hour),
	}, true
}

func listHourMetaKeys(keys []string, collectionBase string) []string {
	var hourKeys []string
	for _, k := range keys {
		if _, ok := ParseHourMetaObjectKey(collectionBase, k); ok {
			hourKeys = append(hourKeys, k)
		}
	}
	return hourKeys
}

// AuditCollection scans OSS hour meta.json files for one collection.
func AuditCollection(ctx context.Context, remote storage.Backend, meta *MetaStore, collectionBase string, ns NSSpec) (*CollectionAuditReport, error) {
	report := &CollectionAuditReport{
		DB:             ns.DB,
		Coll:           ns.Coll,
		CollectionBase: collectionBase,
	}
	cm, found, err := meta.LoadCollectionMeta(ctx, collectionBase)
	if err != nil {
		return nil, err
	}
	report.CollectionFound = found
	report.CollectionMeta = cm
	report.CollectionIssues = auditCollectionIssues(cm)

	prefix := collectionBase + "/"
	keys, err := remote.ListKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, metaKey := range listHourMetaKeys(keys, collectionBase) {
		b, _ := ParseHourMetaObjectKey(collectionBase, metaKey)
		hourBase := HourBase(collectionBase, b)
		hm, ok, err := meta.LoadHourMeta(ctx, hourBase)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", metaKey, err)
		}
		if !ok {
			report.Hours = append(report.Hours, HourAuditRow{
				Bucket:      b,
				HourBase:    hourBase,
				MetaKey:     metaKey,
				StatusValid: false,
				FixHint:     fixHintMissingHourMeta(b),
			})
			continue
		}
		row := hourAuditRowFromMeta(collectionBase, hourBase, metaKey, hm)
		report.Hours = append(report.Hours, row)
	}
	return report, nil
}

func auditCollectionIssues(cm *CollectionMeta) []string {
	if cm == nil {
		return nil
	}
	var issues []string
	if cm.Active != nil {
		if err := validateHourStatus(cm.Active.Status, "collection active"); err != nil {
			issues = append(issues, err.Error()+"; 运行 mongomig repair normalize 或 repair clear-active")
		} else if cm.Active.Status == HourStatusPartial {
			issues = append(issues, fmt.Sprintf("collection active 指向未完成小时 %s (partial)；下次 backup 会从该小时整桶重备", hourRefLabel(cm.Active)))
		}
	}
	if cm.NewestCompleted != nil && cm.Active != nil {
		next := cm.NewestCompleted.Bucket().Next()
		active := cm.Active.Bucket()
		if active.Start.After(next.Start) && !sameHour(active, next) {
			issues = append(issues, fmt.Sprintf("completed 缺口: newest_completed=%s 与 active=%s 之间可能有未备份小时；直接 mongomig backup 会从 %s 起补",
				hourRefLabel(cm.NewestCompleted), hourRefLabel(cm.Active), next.String()))
		}
	}
	return issues
}

func hourAuditRowFromMeta(collectionBase, hourBase, metaKey string, hm *HourMeta) HourAuditRow {
	b := HourBucket{
		Year:  hm.Year,
		Month: hm.Month,
		Day:   hm.Day,
		Hour:  hm.Hour,
		Start: mustParseRFC3339(hm.IntervalStartUTC),
		End:   mustParseRFC3339(hm.IntervalEndUTC),
	}
	if b.Start.IsZero() {
		b, _ = ParseHourMetaObjectKey(collectionBase, metaKey)
	}
	uploaded := countUploaded(hm.Tenants)
	tenantErrs := 0
	for _, t := range hm.Tenants {
		if t.Error != "" {
			tenantErrs++
		}
	}
	valid := validateHourStatus(hm.Status, metaKey) == nil
	row := HourAuditRow{
		Bucket:       b,
		HourBase:     hourBase,
		MetaKey:      metaKey,
		Status:       hm.Status,
		StatusValid:  valid,
		Uploaded:     uploaded,
		Total:        len(hm.Tenants),
		TenantErrors: tenantErrs,
	}
	row.FixHint = fixHintForHour(row)
	return row
}

func fixHintMissingHourMeta(b HourBucket) string {
	return fmt.Sprintf("OSS 上有 meta.json 路径但读失败；用 --from-hour %s --to-hour %s 重跑（partial 会自动整桶重备）", b.String(), b.String())
}

func fixHintForHour(row HourAuditRow) string {
	h := row.Bucket.String()
	if !row.StatusValid {
		return fmt.Sprintf("非法 status=%q 会阻止 backup；先 mongomig repair normalize，再 mongomig backup --from-hour %s --to-hour %s",
			row.Status, h, h)
	}
	if row.Status == HourStatusComplete {
		if row.TenantErrors > 0 {
			return fmt.Sprintf("虽为 complete 但有 %d 个租户 error；若需重备: mongomig backup --force-hour --from-hour %s --to-hour %s", row.TenantErrors, h, h)
		}
		return ""
	}
	// partial
	parts := []string{
		fmt.Sprintf("status=partial（已上传 %d/%d）", row.Uploaded, row.Total),
	}
	if row.TenantErrors > 0 {
		parts = append(parts, fmt.Sprintf("%d 个租户带 error", row.TenantErrors))
	}
	parts = append(parts, fmt.Sprintf("修复: mongomig backup --db ... --collections ... --from-hour %s --to-hour %s（partial 会自动 DeletePrefix 整小时重备）", h, h))
	return strings.Join(parts, "; ")
}

// IncompleteHours returns hour rows that are not successfully complete (or have issues).
func (r *CollectionAuditReport) IncompleteHours() []HourAuditRow {
	var out []HourAuditRow
	for _, h := range r.Hours {
		if h.Status != HourStatusComplete || !h.StatusValid || h.TenantErrors > 0 {
			out = append(out, h)
		}
	}
	return out
}

// NormalizeInvalidHourMetas rewrites invalid hour status to partial so backup can proceed.
func NormalizeInvalidHourMetas(ctx context.Context, meta *MetaStore, remote storage.Backend, collectionBase string) (int, error) {
	keys, err := remote.ListKeys(ctx, collectionBase+"/")
	if err != nil {
		return 0, err
	}
	fixed := 0
	for _, metaKey := range listHourMetaKeys(keys, collectionBase) {
		b, ok := ParseHourMetaObjectKey(collectionBase, metaKey)
		if !ok {
			continue
		}
		hourBase := HourBase(collectionBase, b)
		hm, found, err := meta.LoadHourMeta(ctx, hourBase)
		if err != nil || !found {
			return fixed, err
		}
		if validateHourStatus(hm.Status, metaKey) == nil {
			continue
		}
		hm.Status = HourStatusPartial
		if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
			return fixed, err
		}
		fixed++
	}
	return fixed, nil
}

// NormalizeCollectionActiveStatus sets invalid collection active.status to partial.
func NormalizeCollectionActiveStatus(ctx context.Context, meta *MetaStore, collectionBase string) (bool, error) {
	cm, found, err := meta.LoadCollectionMeta(ctx, collectionBase)
	if err != nil || !found || cm == nil || cm.Active == nil {
		return false, err
	}
	if validateHourStatus(cm.Active.Status, "collection active") == nil {
		return false, nil
	}
	cm.Active.Status = HourStatusPartial
	return true, meta.SaveCollectionMeta(ctx, collectionBase, cm)
}

// ClearCollectionActive removes collection meta active pointer.
func ClearCollectionActive(ctx context.Context, meta *MetaStore, collectionBase string) (bool, error) {
	cm, found, err := meta.LoadCollectionMeta(ctx, collectionBase)
	if err != nil || !found || cm == nil || cm.Active == nil {
		return false, err
	}
	cm.Active = nil
	return true, meta.SaveCollectionMeta(ctx, collectionBase, cm)
}
