package main

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/evstack/apex/pkg/api"
)

func statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show indexer sync status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			addr, _ := cmd.Flags().GetString("rpc-addr")
			format, _ := cmd.Flags().GetString("format")

			client := newRPCClient(addr)
			raw, err := client.fetchHealth(cmd.Context())
			if err != nil {
				return fmt.Errorf("fetch status: %w", err)
			}

			if format == "table" {
				var hs api.HealthStatus
				if err := json.Unmarshal(raw, &hs); err != nil {
					return fmt.Errorf("decode status: %w", err)
				}
				return printStatusTable(&hs)
			}

			// JSON output (pretty-printed).
			var out any
			if err := json.Unmarshal(raw, &out); err != nil {
				return fmt.Errorf("decode status: %w", err)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		},
	}
	return cmd
}

func printStatusTable(hs *api.HealthStatus) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, row := range []struct {
		label string
		value any
	}{
		{"Healthy", hs.Healthy},
		{"Sync State", hs.SyncState},
		{"Latest Height", hs.LatestHeight},
		{"Network Height", hs.NetworkHeight},
		{"Sync Lag", hs.SyncLag},
		{"Uptime", hs.Uptime},
		{"Version", hs.Version},
		{"Subscribers", hs.Subscribers},
	} {
		if _, err := fmt.Fprintf(w, "%s:\t%v\n", row.label, row.value); err != nil {
			return err
		}
	}
	return w.Flush()
}
