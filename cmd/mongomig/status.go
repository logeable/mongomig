package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/logeable/mongomig/internal/backup"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newStatusCmd(v *viper.Viper) *cobra.Command {
	var dbName, collections string
	var showAll bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Scan OSS hour meta.json and list non-complete or invalid hours",
		Long: `列出 OSS 上各 collection 的小时 meta 状态，重点标出 partial、非法 status（如历史 in_progress）、
以及 collection meta 上的 active/缺口问题，并给出修复命令建议。

默认只显示「有问题」的小时；加 --all 显示全部已发现的小时分区。`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			run, err := prepareOSSRun(ctx, v, dbName, collections)
			if err != nil {
				return err
			}
			defer run.close()

			var totalIssues int
			for _, ns := range run.specs {
				collBase := backup.CollectionBase(run.cfg.RemotePrefix, ns.DB, ns.Coll)
				report, err := backup.AuditCollection(ctx, run.remote, run.meta, collBase, ns)
				if err != nil {
					return err
				}
				printCollectionAudit(os.Stdout, report, showAll, &totalIssues)
			}
			if totalIssues == 0 {
				fmt.Println("\n未发现需要处理的小时 meta（或仅存在 complete 且租户无 error）。")
			} else {
				fmt.Printf("\n共 %d 条需关注项（含 collection 级提示）。\n", totalIssues)
				printGlobalFixGuide()
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dbName, "db", "", "MongoDB database name")
	cmd.Flags().StringVar(&collections, "collections", "", "Comma-separated collections; default discover all")
	cmd.Flags().BoolVar(&showAll, "all", false, "List every hour meta on OSS, not only problematic ones")
	_ = v.BindPFlag("db", cmd.Flags().Lookup("db"))
	return cmd
}

func printCollectionAudit(w *os.File, report *backup.CollectionAuditReport, showAll bool, totalIssues *int) {
	fmt.Printf("\n=== %s/%s  (%s) ===\n", report.DB, report.Coll, report.CollectionBase)
	if !report.CollectionFound {
		fmt.Println("  collection meta.json: 不存在")
	} else if report.CollectionMeta != nil {
		cm := report.CollectionMeta
		fmt.Printf("  collection meta: updated=%s\n", cm.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"))
		if cm.OldestCompleted != nil {
			fmt.Printf("    oldest_completed: %s\n", hourRefShort(cm.OldestCompleted))
		}
		if cm.NewestCompleted != nil {
			fmt.Printf("    newest_completed: %s\n", hourRefShort(cm.NewestCompleted))
		}
		if cm.Active != nil {
			fmt.Printf("    active: %s\n", hourRefShort(cm.Active))
		}
	}
	for _, issue := range report.CollectionIssues {
		fmt.Printf("  [!] %s\n", issue)
		*totalIssues++
	}

	rows := report.Hours
	if !showAll {
		rows = report.IncompleteHours()
	}
	if len(rows) == 0 {
		if showAll {
			fmt.Println("  (无小时 meta)")
		} else {
			fmt.Println("  (无异常小时 meta)")
		}
		return
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "HOUR\tSTATUS\tVALID\tUPLOADED\tERRORS\tFIX")
	for _, row := range rows {
		valid := "yes"
		if !row.StatusValid {
			valid = "NO"
			*totalIssues++
		} else if row.Status != backup.HourStatusComplete {
			*totalIssues++
		} else if row.TenantErrors > 0 {
			*totalIssues++
		}
		fix := row.FixHint
		if fix == "" {
			fix = "-"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%d\t%s\n",
			row.Bucket.String(),
			row.Status,
			valid,
			row.Uploaded,
			row.Total,
			row.TenantErrors,
			fix,
		)
	}
	_ = tw.Flush()
}

func hourRefShort(r *backup.HourRef) string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02dT%02dZ status=%s", r.Year, r.Month, r.Day, r.Hour, r.Status)
}

func printGlobalFixGuide() {
	fmt.Println(`
修复说明（与当前 backup 行为一致）:
  • partial 小时: backup 遇到时会 DeletePrefix 整小时后全量重备，无需手改 meta。
    例: mongomig backup --db <db> --collections <c> --from-hour 2026-05-18T07 --to-hour 2026-05-18T07
  • 非法 status (如 in_progress): backup 会直接失败；先执行:
    mongomig repair normalize --db <db> --collections <c>
    再对对应小时跑 backup（同上 --from-hour/--to-hour）。
  • 想清空某小时 OSS 数据: mongomig backup --reset-hour 2026-05-18T07 再 backup。
  • collection active 非法: repair normalize 会把 active.status 改为 partial；或 repair clear-active 清空 active。
  • newest_completed 与 active 之间有缺口: 直接 mongomig backup，会从 newest_completed+1 顺序补。`)
}
