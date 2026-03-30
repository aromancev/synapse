package cmd

import (
	"encoding/json"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/spf13/cobra"
)

var nodesKeywordsUpdateCmd = &cobra.Command{
	Use:   "update <node-id> [json-keywords]",
	Short: "Update node keywords",
	Args:  cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeID, err := nodes.ParseID(args[0])
		if err != nil {
			return err
		}

		payloadJSON, err := readJSONPayload(args[1:])
		if err != nil {
			return err
		}

		var keywords []string
		if err := json.Unmarshal([]byte(payloadJSON), &keywords); err != nil {
			return err
		}
		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.UpdateNodeKeywords(cmd.Context(), nodeID, keywords); err != nil {
			return err
		}
		if cfg.Replication.Mode == config.ReplicationModeAuto {
			if err := service.RunReplication(cmd.Context()); err != nil {
				return err
			}
		}
		if err := service.RunProjections(cmd.Context()); err != nil {
			return err
		}

		return writeOK("node_keywords_updated", map[string]any{
			"id":       nodeID.String(),
			"keywords": keywords,
		})
	},
}

func init() {
	nodesKeywordsCmd.AddCommand(nodesKeywordsUpdateCmd)
}
