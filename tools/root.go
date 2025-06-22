package main

import (
	"github.com/lazygophers/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "lmqtools",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		log.SetTrace()
	},
}

func main() {
	var err error

	cobra.EnablePrefixMatching = true
	cobra.EnableCommandSorting = true
	cobra.EnableTraverseRunHooks = true

	err = rootCmd.Execute()
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
}
