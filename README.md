# lmq

异步消息通信，常用于应用程序及服务器之间，与不同语言环境等的设备进行交互，消息队列缓存发送方发送的消息，以便接收方需要时接收，防止连接失败或者任意一方系统崩溃造成的消息丢失，具有跨平台设备语言、稳定性、兼容性、异步安全性。

- 基础功能

1. 接收发送方发送的消息并根据消息动作进行分类、存入消息队列主题
2. 消息队列根据优先级创建发送通道，传递消息，同步发送到多个端
3. 等待接收方从通道中接收，获得消费成功信息表示该消息异步通信成功

- 应用场景需求
    1. 确保消息存入磁盘，避免消息丢失
    2. 兼容不同平台环境、多编程语言的终端设备
    3. 监听传递过程发送情况，以便及时跟踪调整网络等环境情况
    4. 性能好，避免堆积，及时确认

## 模块设计

### 流程

``` mermaid
flowchart TD
	Producer -- Message --> Topic
	Topic -- Message --> Channel1 & Channeln
	Channel1 -- Message --> Consumer1 & Consumer1n
	Channeln -- Message --> Consumer2 & Consumer2n
```

### 模块划分

##### Message

`id`

`created_at`

`data`

`extr`

##### Queue

> Topic和Channel的底层数据结构，提供基础操作

`newQueue(name) Queue`

`clean()`

`pushMessage(msg Message)`

`popMessage() Message`

##### Channel

`pushMessage(msg Message)`

`popMessage() Message`

`consume()`

`clean()`

##### Topic

`newChannel(name)`

`getChannel(name)`

`delChannel(name)`

`listChannel()`

`pushMessage(msg Message)`

##### TopicManage

`newTopic(name)`

`getTopic(name)`

`listTopic()`

`delTopic()`

`pushMessage(msg Message)`







