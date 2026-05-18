package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newRestoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "restore",
		Short: "Restore from OSS hourly backups (not implemented)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("mongomig restore 尚未实现")
		},
	}
}
