package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

func blobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "blob",
		Short: "Query blobs from the indexer",
	}
	cmd.AddCommand(blobGetCmd())
	cmd.AddCommand(blobListCmd())
	return cmd
}

func blobGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <height> <namespace-hex> <commitment-hex>",
		Short: "Get a single blob by height, namespace, and commitment",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			addr, _ := cmd.Flags().GetString("rpc-addr")

			height, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid height: %w", err)
			}

			ns, err := hex.DecodeString(args[1])
			if err != nil {
				return fmt.Errorf("invalid namespace hex: %w", err)
			}

			commitment, err := hex.DecodeString(args[2])
			if err != nil {
				return fmt.Errorf("invalid commitment hex: %w", err)
			}

			client := newRPCClient(addr)
			result, err := client.call(cmd.Context(), "blob.Get", height, ns, commitment)
			if err != nil {
				return err
			}

			return printJSON(cmd, result)
		},
	}
}

func blobListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <height>",
		Short: "List all blobs at a given height",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			addr, _ := cmd.Flags().GetString("rpc-addr")
			nsHex, _ := cmd.Flags().GetString("namespace")

			height, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid height: %w", err)
			}

			var namespaces [][]byte
			if nsHex != "" {
				ns, err := hex.DecodeString(nsHex)
				if err != nil {
					return fmt.Errorf("invalid namespace hex: %w", err)
				}
				namespaces = [][]byte{ns}
			}

			client := newRPCClient(addr)
			result, err := client.call(cmd.Context(), "blob.GetAll", height, namespaces)
			if err != nil {
				return err
			}

			return printJSON(cmd, result)
		},
	}

	cmd.Flags().String("namespace", "", "filter by namespace (hex-encoded)")
	return cmd
}

func printJSON(cmd *cobra.Command, raw json.RawMessage) error {
	format, _ := cmd.Flags().GetString("format")
	if format == "table" {
		// For blob data, table format just pretty-prints the JSON since blob
		// fields are dynamic.
		return prettyPrintJSON(raw)
	}
	return prettyPrintJSON(raw)
}

func prettyPrintJSON(raw json.RawMessage) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(raw)
}
