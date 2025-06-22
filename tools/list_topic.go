package main

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/log"
	"github.com/lazygophers/lrpc"
	"github.com/spf13/cobra"
)

var listTopicCmd = &cobra.Command{
	Use: "list-topic",
	RunE: func(c *cobra.Command, args []string) error {
		rsp, err := lmq.ListTopic(lrpc.NewCtxTools(), &lmq.ListTopicReq{})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		log.GenTraceId()

		log.Info(rsp.TopicList)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(listTopicCmd)
}
