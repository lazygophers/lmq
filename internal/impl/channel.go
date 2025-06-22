package impl

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/lmq/internal/state"
	"github.com/lazygophers/log"
	"github.com/lazygophers/lrpc"
	"github.com/lazygophers/lrpc/middleware/xerror"
)

func SetChannel(ctx *lrpc.Ctx, req *lmq.SetChannelReq) (*lmq.SetChannelRsp, error) {
	var rsp lmq.SetChannelRsp

	topic := state.State.TopicManage.GetTopic(req.TopicName)
	if topic == nil {
		log.Errorf("not found topic:%s", req.TopicName)
		return nil, xerror.NewError(int32(lmq.ErrCode_TopicNotFound))
	}

	err := topic.SetChannel(req.Channel)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetChannel(ctx *lrpc.Ctx, req *lmq.GetChannelReq) (*lmq.GetChannelRsp, error) {
	var rsp lmq.GetChannelRsp

	topic := state.State.TopicManage.GetTopic(req.TopicName)
	if topic == nil {
		log.Errorf("not found topic:%s", req.TopicName)
		return nil, xerror.NewError(int32(lmq.ErrCode_TopicNotFound))
	}

	channel := topic.GetChannel(req.ChannelName)
	if channel == nil {
		log.Errorf("not found channel:%s", req.ChannelName)
		return nil, xerror.NewError(int32(lmq.ErrCode_ChannelNotFound))
	}

	rsp.Channel = channel.Channel()

	return &rsp, nil
}

func DeleteChannel(ctx *lrpc.Ctx, req *lmq.DeleteChannelReq) (*lmq.DeleteChannelRsp, error) {
	var rsp lmq.DeleteChannelRsp

	topic := state.State.TopicManage.GetTopic(req.TopicName)
	if topic == nil {
		log.Errorf("not found topic:%s", req.TopicName)
		return nil, xerror.NewError(int32(lmq.ErrCode_TopicNotFound))
	}

	err := topic.DelChannel(req.ChannelName)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
