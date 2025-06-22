package impl

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/lmq/internal/state"
	"github.com/lazygophers/log"
	"github.com/lazygophers/lrpc"
	"github.com/lazygophers/lrpc/middleware/xerror"
)

func Pub(ctx *lrpc.Ctx, req *lmq.PubReq) (*lmq.PubRsp, error) {
	var rsp lmq.PubRsp

	var queue state.QueueInterface

	topic := state.State.TopicManage.GetTopic(req.TopicName)
	if topic == nil {
		log.Errorf("not found topic:%s", req.TopicName)
		return nil, xerror.NewError(int32(lmq.ErrCode_TopicNotFound))
	}

	queue = topic

	if req.ChannelName != "" {
		channel := topic.GetChannel(req.ChannelName)
		if channel == nil {
			log.Errorf("not found channel:%s", req.ChannelName)
			return nil, xerror.NewError(int32(lmq.ErrCode_ChannelNotFound))
		}

		queue = channel
	}

	err := queue.PubMessage(req.Msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.MsgId = req.Msg.MsgId

	return &rsp, nil
}

func BatchPub(ctx *lrpc.Ctx, req *lmq.BatchPubReq) (*lmq.BatchPubRsp, error) {
	var rsp lmq.BatchPubRsp

	var queue *state.Queue

	topic := state.State.TopicManage.GetTopic(req.TopicName)
	if topic == nil {
		log.Errorf("not found topic:%s", req.TopicName)
		return nil, xerror.NewError(int32(lmq.ErrCode_TopicNotFound))
	}

	queue = topic.Queue

	if req.ChannelName != "" {
		channel := topic.GetChannel(req.ChannelName)
		if channel == nil {
			log.Errorf("not found channel:%s", req.ChannelName)
			return nil, xerror.NewError(int32(lmq.ErrCode_ChannelNotFound))
		}

		queue = channel.Queue
	}

	for _, msg := range req.MsgList {
		err := queue.PubMessage(msg)
		if err != nil {
			log.Errorf("err:%v", err)
			if req.BreakWhenError {
				return &rsp, err
			}
		} else {
			rsp.MsgIdList = append(rsp.MsgIdList, msg.MsgId)
		}
	}

	return &rsp, nil
}
