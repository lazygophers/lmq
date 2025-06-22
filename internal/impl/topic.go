package impl

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/lmq/internal/state"
	"github.com/lazygophers/log"
	"github.com/lazygophers/lrpc"
	"github.com/lazygophers/lrpc/middleware/xerror"
)

func SetTopic(ctx *lrpc.Ctx, req *lmq.SetTopicReq) (*lmq.SetTopicRsp, error) {
	var rsp lmq.SetTopicRsp

	err := state.State.TopicManage.SetTopic(req.Topic)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetTopic(ctx *lrpc.Ctx, req *lmq.GetTopicReq) (*lmq.GetTopicRsp, error) {
	var rsp lmq.GetTopicRsp

	topic := state.State.TopicManage.GetTopic(req.TopicName)
	if topic == nil {
		log.Errorf("not found topic:%s", req.TopicName)
		return nil, xerror.NewError(int32(lmq.ErrCode_TopicNotFound))
	}

	rsp.Topic = topic.Topic()

	if req.ShowDepth {
		rsp.Depth = topic.Depth()
	}

	return &rsp, nil
}

func DeleteTopic(ctx *lrpc.Ctx, req *lmq.DeleteTopicReq) (*lmq.DeleteTopicRsp, error) {
	var rsp lmq.DeleteTopicRsp

	return &rsp, nil
}
func ListTopic(ctx *lrpc.Ctx, req *lmq.ListTopicReq) (*lmq.ListTopicRsp, error) {
	var rsp lmq.ListTopicRsp

	for _, topic := range state.State.TopicManage.TopicList() {
		rsp.TopicList = append(rsp.TopicList, topic.Topic())
	}

	return &rsp, nil
}
