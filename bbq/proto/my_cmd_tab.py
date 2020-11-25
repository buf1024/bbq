from bbq.proto.bbq_pb2 import *
    
heartbeat_req = 0x000001
heartbeat_rsp = 0x000002
quot_subscribe_req = 0x010001
quot_subscribe_rsp = 0x010002
quot_unsubscribe_req = 0x010003
quot_unsubscribe_rsp = 0x010004
quot_dispatch_req = 0x010005
quot_dispatch_rsp = 0x010006
trade_order_req = 0x020001
trade_order_rsp = 0x020002


cmd_class = {
    quot_subscribe_req: QuotSubscribeReq,
    quot_subscribe_rsp: QuotSubscribeRsp,
    quot_unsubscribe_req: QuotUnsubscribeReq,
    quot_unsubscribe_rsp: QuotUnsubscribeRsp,
    quot_dispatch_req: QuotDispatchReq,
    quot_dispatch_rsp: QuotDispatchRsp,
    trade_order_req: TradeOrderReq,
    trade_order_rsp: TradeOrderRsp,

}

cmd_name = {
    heartbeat_req: "HeartbeatReq",
    heartbeat_rsp: "HeartbeatRsp",
    quot_subscribe_req: "QuotSubscribeReq",
    quot_subscribe_rsp: "QuotSubscribeRsp",
    quot_unsubscribe_req: "QuotUnsubscribeReq",
    quot_unsubscribe_rsp: "QuotUnsubscribeRsp",
    quot_dispatch_req: "QuotDispatchReq",
    quot_dispatch_rsp: "QuotDispatchRsp",
    trade_order_req: "TradeOrderReq",
    trade_order_rsp: "TradeOrderRsp",

}

