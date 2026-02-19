package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/evstack/apex/config"
)

func configCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Config management commands",
	}
	cmd.AddCommand(configValidateCmd())
	cmd.AddCommand(configShowCmd())
	return cmd
}

func configValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the config file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgPath, err := configPath(cmd)
			if err != nil {
				return err
			}
			_, err = config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("config invalid: %w", err)
			}
			fmt.Println("config is valid")
			return nil
		},
	}
}

func configShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Show the effective config",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgPath, err := configPath(cmd)
			if err != nil {
				return err
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			enc := yaml.NewEncoder(os.Stdout)
			enc.SetIndent(2)
			if err := enc.Encode(cfg); err != nil {
				return fmt.Errorf("encoding config: %w", err)
			}
			return enc.Close()
		},
	}
}
