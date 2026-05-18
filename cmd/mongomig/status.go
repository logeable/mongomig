package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show backup status from collection meta on OSS (not implemented)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("mongomig status 尚未实现")
		},
	}
}
