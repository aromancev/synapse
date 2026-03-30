package cmd

import "github.com/spf13/cobra"

var replicationCmd = &cobra.Command{
	Use:   "replication",
	Short: "Manage replication",
}

var replicationRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run replication",
	RunE: func(cmd *cobra.Command, args []string) error {
		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.RunReplication(cmd.Context()); err != nil {
			return err
		}
		return writeOK("replication_ran", nil)
	},
}

var replicationRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore event store from configured replicator",
	RunE: func(cmd *cobra.Command, args []string) error {
		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.Restore(cmd.Context()); err != nil {
			return err
		}
		if err := service.RunProjections(cmd.Context()); err != nil {
			return err
		}
		return writeOK("replication_restored", nil)
	},
}

func init() {
	replicationCmd.AddCommand(replicationRunCmd)
	replicationCmd.AddCommand(replicationRestoreCmd)
	rootCmd.AddCommand(replicationCmd)
}
