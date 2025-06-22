package impl

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/lmq/internal/state"
	"github.com/lazygophers/log"
	"github.com/lazygophers/lrpc"
	"github.com/lazygophers/lrpc/middleware/xerror"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func PopMessage(ctx *lrpc.Ctx, req *lmq.PopMessageReq) (*lmq.PopMessageRsp, error) {
	var rsp lmq.PopMessageRsp

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

	msg := channel.Pop()

	if msg == nil {
		return nil, xerror.NewError(int32(lmq.ErrCode_QueueEmpty))
	}

	rsp.Msg = &lmq.QueuePop{
		CreatedAt:     msg.CreatedAt,
		RetryCount:    msg.RetryCount,
		Data:          msg.Data,
		MsgId:         msg.MsgId,
		MaxRetryCount: channel.Channel().MaxRetryCount,
	}

	return &rsp, nil
}

func FinishMessage(ctx *lrpc.Ctx, req *lmq.FinishMessageReq) (*lmq.FinishMessageRsp, error) {
	var rsp lmq.FinishMessageRsp

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

	msg := req.Msg
	if msg.NeedRetry {
		m := channel.GetInFlight(msg.MsgId)

		if !msg.SkipRetryCount {
			m.RetryCount++
		}

		if msg.RetryWait != nil {
			m.ExecAt = timestamppb.New(time.Now().Add(msg.RetryWait.AsDuration()))
		}

		err := channel.Requeue(m)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

	} else {
		channel.Finish(msg.MsgId)
	}

	return &rsp, nil
}
