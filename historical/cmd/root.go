package cmd

import (
	"ftxu/cmd/download"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "ftxu",
	Short: "ftxu related tools",
}

func init() {
	rootCmd.AddCommand(download.Cmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
