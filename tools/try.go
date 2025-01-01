package main

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/log"
	"github.com/lazygophers/lrpc"
	"github.com/lazygophers/utils/unit"
	"github.com/spf13/cobra"
)

var tryCmd = &cobra.Command{
	Use: "try",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		ctx := lrpc.NewCtxTools()

		const (
			Topic    = "try"
			Channel1 = "try-1"
			Channel2 = "try-2"
		)

		_, err = lmq.SetTopic(ctx, &lmq.SetTopicReq{
			Topic: &lmq.Topic{
				Name:        "try",
				ChannelList: nil,
				DiskQueue: &lmq.DiskQueue{
					MaxFilePartSize: unit.MB * 100,
				},
				MaxMsgSize: unit.GB,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		{
			rsp, err := lmq.GetTopic(ctx, &lmq.GetTopicReq{
				TopicName: Topic,
				ShowDepth: true,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			log.Info(rsp.Topic)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(tryCmd)
}
