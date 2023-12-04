/**
 * Created by lock
 * Date: 2019-08-13
 * Time: 10:50
 */
package task

import (
	"encoding/json"
	"gochat/config"
	"gochat/proto"
	"math/rand"

	"github.com/sirupsen/logrus"
)

type PushParams struct {
	ServerId string
	UserId   int
	Msg      []byte
	RoomId   int
}

// 使用一个固定长度的PushParams数组来做负载均衡
var pushChannel []chan *PushParams

func init() {
	pushChannel = make([]chan *PushParams, config.Conf.Task.TaskBase.PushChan)
}

func (task *Task) GoPush() {
	for i := 0; i < len(pushChannel); i++ {
		pushChannel[i] = make(chan *PushParams, config.Conf.Task.TaskBase.PushChanSize)
		go task.processSinglePush(pushChannel[i])
	}
}

func (task *Task) processSinglePush(ch chan *PushParams) {
	var arg *PushParams
	for {
		arg = <-ch
		//@todo when arg.ServerId server is down, user could be reconnect other serverId but msg in queue no consume
		task.pushSingleToConnect(arg.ServerId, arg.UserId, arg.Msg)
	}
}

// Push：处理REDIS队列里的协议消息
func (task *Task) Push(msg string) {
	m := &proto.RedisMsg{}
	if err := json.Unmarshal([]byte(msg), m); err != nil {
		logrus.Infof(" json.Unmarshal err:%v ", err)
	}
	logrus.Infof("push msg info %d,op is:%d", m.RoomId, m.Op)

	//协议类型

	// config.OpSingleSend这种类型与其他三种类型不一样的是：
	// config.OpSingleSend已知CONNECT服务器的ID，所以可以直接拿到CONNECT服务器的RPC客户端长连接，然后发送消息
	// 其他三种，因为不知道具体在哪台服务器上，所以只能通过广播的方式发送
	// 这里的隐含条件是，某个用户/客户端（userid）只能连接到某个CONNECT服务器上
	switch m.Op {
	case config.OpSingleSend:
		//随机push到某个channel中
		pushChannel[rand.Int()%config.Conf.Task.TaskBase.PushChan] <- &PushParams{
			ServerId: m.ServerId,
			UserId:   m.UserId,
			Msg:      m.Msg,
		}
	case config.OpRoomSend:
		// 向房间room广播消息msg
		task.broadcastRoomToConnect(m.RoomId, m.Msg)
	case config.OpRoomCountSend:
		task.broadcastRoomCountToConnect(m.RoomId, m.Count)
	case config.OpRoomInfoSend:
		// 房间信息（人员列表）推送
		task.broadcastRoomInfoToConnect(m.RoomId, m.RoomUserInfo)
	}
}
