package lmq

import (
	"github.com/lazygophers/lrpc"
	"github.com/lazygophers/lrpc/middleware/core"
	"github.com/lazygophers/lrpc/middleware/service_discovery/ldiscovery"
)

func init() {
	lrpc.DiscoveryClient = ldiscovery.DiscoveryClient
}

func ListTopic(ctx *lrpc.Ctx, req *ListTopicReq) (*ListTopicRsp, error) {
	var rsp ListTopicRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathListTopic,
	}, req, &rsp)
}

func SetTopic(ctx *lrpc.Ctx, req *SetTopicReq) (*SetTopicRsp, error) {
	var rsp SetTopicRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathSetTopic,
	}, req, &rsp)
}

func GetTopic(ctx *lrpc.Ctx, req *GetTopicReq) (*GetTopicRsp, error) {
	var rsp GetTopicRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathGetTopic,
	}, req, &rsp)
}

func DeleteTopic(ctx *lrpc.Ctx, req *DeleteTopicReq) (*DeleteTopicRsp, error) {
	var rsp DeleteTopicRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathDeleteTopic,
	}, req, &rsp)
}

func SetChannel(ctx *lrpc.Ctx, req *SetChannelReq) (*SetChannelRsp, error) {
	var rsp SetChannelRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathSetChannel,
	}, req, &rsp)
}

func GetChannel(ctx *lrpc.Ctx, req *GetChannelReq) (*GetChannelRsp, error) {
	var rsp GetChannelRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathGetChannel,
	}, req, &rsp)
}

func DeleteChannel(ctx *lrpc.Ctx, req *DeleteChannelReq) (*DeleteChannelRsp, error) {
	var rsp DeleteChannelRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathDeleteChannel,
	}, req, &rsp)
}

func Pub(ctx *lrpc.Ctx, req *PubReq) (*PubRsp, error) {
	var rsp PubRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathPub,
	}, req, &rsp)
}

func BatchPub(ctx *lrpc.Ctx, req *BatchPubReq) (*BatchPubRsp, error) {
	var rsp BatchPubRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathBatchPub,
	}, req, &rsp)
}

func PopMessage(ctx *lrpc.Ctx, req *PopMessageReq) (*PopMessageRsp, error) {
	var rsp PopMessageRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathPopMessage,
	}, req, &rsp)
}

func FinishMessage(ctx *lrpc.Ctx, req *FinishMessageReq) (*FinishMessageRsp, error) {
	var rsp FinishMessageRsp
	return &rsp, lrpc.Call(ctx, &core.ServiceDiscoveryClient{
		ServiceName: ServerName,
		ServicePath: RpcPathFinishMessage,
	}, req, &rsp)
}
