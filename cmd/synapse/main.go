package main

import (
	"os"

	"github.com/aromancev/synapse/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
