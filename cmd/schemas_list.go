package cmd

import "github.com/spf13/cobra"

var listArchivedSchemas bool

var schemasListCmd = &cobra.Command{
	Use:   "list",
	Short: "List schemas",
	RunE: func(cmd *cobra.Command, args []string) error {
		service, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if listArchivedSchemas {
			storedSchemas, err := service.GetArchivedSchemas(cmd.Context())
			if err != nil {
				return err
			}
			return writeJSON(storedSchemas)
		}

		storedSchemas, err := service.GetSchemas(cmd.Context())
		if err != nil {
			return err
		}
		return writeJSON(storedSchemas)
	},
}

func init() {
	schemasListCmd.Flags().BoolVar(&listArchivedSchemas, "archived", false, "list archived schemas instead of live ones")
	schemasCmd.AddCommand(schemasListCmd)
}
