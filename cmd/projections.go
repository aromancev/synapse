package cmd

import "github.com/spf13/cobra"

var projectionsCmd = &cobra.Command{
	Use:   "projections",
	Short: "Manage projections",
}

var projectionsRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run all projections",
	RunE: func(cmd *cobra.Command, args []string) error {
		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.RunProjections(cmd.Context()); err != nil {
			return err
		}
		return writeOK("projections_ran", nil)
	},
}

func init() {
	projectionsCmd.AddCommand(projectionsRunCmd)
	rootCmd.AddCommand(projectionsCmd)
}
