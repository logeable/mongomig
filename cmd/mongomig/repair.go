package main

import (
	"fmt"

	"github.com/logeable/mongomig/internal/backup"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newRepairCmd(v *viper.Viper) *cobra.Command {
	var dbName, collections string
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "Fix OSS meta blockers (invalid hour/collection status) after Ctrl+C",
	}
	cmd.AddCommand(newRepairNormalizeCmd(v, &dbName, &collections))
	cmd.AddCommand(newRepairClearActiveCmd(v, &dbName, &collections))
	return cmd
}

func newRepairNormalizeCmd(v *viper.Viper, dbName, collections *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "normalize",
		Short: "Set invalid hour meta status (e.g. in_progress) to partial; fix collection active.status",
		Long: `将 OSS 上非法的小时 status（非 partial/complete，常见于旧版 in_progress）改为 partial，
并将 collection meta 里 active.status 的非法值改为 partial，以便后续 mongomig backup 能继续。

不会删除 dump.tar；partial 小时仍由 backup 整桶 DeletePrefix 后重备。`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			run, err := prepareOSSRun(ctx, v, *dbName, *collections)
			if err != nil {
				return err
			}
			defer run.close()

			var hourFixed, collFixed int
			for _, ns := range run.specs {
				collBase := backup.CollectionBase(run.cfg.RemotePrefix, ns.DB, ns.Coll)
				n, err := backup.NormalizeInvalidHourMetas(ctx, run.meta, run.remote, collBase)
				if err != nil {
					return err
				}
				hourFixed += n
				ok, err := backup.NormalizeCollectionActiveStatus(ctx, run.meta, collBase)
				if err != nil {
					return err
				}
				if ok {
					collFixed++
				}
				if n > 0 || ok {
					fmt.Printf("%s/%s: 小时 meta 修正 %d 个", ns.DB, ns.Coll, n)
					if ok {
						fmt.Print(", collection active.status 已改为 partial")
					}
					fmt.Println()
				}
			}
			if hourFixed == 0 && collFixed == 0 {
				fmt.Println("未发现需要 normalize 的非法 status。")
			} else {
				fmt.Printf("完成: 修正 %d 个小时 meta, %d 个 collection active.status\n", hourFixed, collFixed)
				fmt.Println("下一步: 对 partial/未完成小时执行 mongomig backup（见 mongomig status 中的 FIX 列）。")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(dbName, "db", "", "MongoDB database name")
	cmd.Flags().StringVar(collections, "collections", "", "Comma-separated collections")
	_ = v.BindPFlag("db", cmd.Flags().Lookup("db"))
	return cmd
}

func newRepairClearActiveCmd(v *viper.Viper, dbName, collections *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear-active",
		Short: "Remove collection meta active pointer (backup uses newest_completed+1)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			run, err := prepareOSSRun(ctx, v, *dbName, *collections)
			if err != nil {
				return err
			}
			defer run.close()

			var cleared int
			for _, ns := range run.specs {
				collBase := backup.CollectionBase(run.cfg.RemotePrefix, ns.DB, ns.Coll)
				ok, err := backup.ClearCollectionActive(ctx, run.meta, collBase)
				if err != nil {
					return err
				}
				if ok {
					cleared++
					fmt.Printf("%s/%s: active 已清除\n", ns.DB, ns.Coll)
				}
			}
			if cleared == 0 {
				fmt.Println("没有 collection 带有 active 字段。")
			} else {
				fmt.Printf("已清除 %d 个 collection 的 active；请运行 mongomig backup 从 newest_completed+1 继续。\n", cleared)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(dbName, "db", "", "MongoDB database name")
	cmd.Flags().StringVar(collections, "collections", "", "Comma-separated collections")
	_ = v.BindPFlag("db", cmd.Flags().Lookup("db"))
	return cmd
}
