/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 15:18
 */
package connect

import (
	"gochat/proto"
	"sync"
	"sync/atomic"
)

// 一个Bucket，管理多个room、管理多个会话（Channel）
// chs romms这两个成员
type Bucket struct {
	cLock         sync.RWMutex     // protect the channels for chs
	chs           map[int]*Channel // map sub key to a channel
	bucketOptions BucketOptions
	rooms         map[int]*Room // bucket room channels ，key：roomid；value：Room
	routines      []chan *proto.PushRoomMsgRequest
	routinesNum   uint64
	broadcast     chan []byte
}

type BucketOptions struct {
	ChannelSize   int
	RoomSize      int
	RoutineAmount uint64
	RoutineSize   int
}

func NewBucket(bucketOptions BucketOptions) (b *Bucket) {
	b = new(Bucket)
	b.chs = make(map[int]*Channel, bucketOptions.ChannelSize)
	b.bucketOptions = bucketOptions
	b.routines = make([]chan *proto.PushRoomMsgRequest, bucketOptions.RoutineAmount)
	b.rooms = make(map[int]*Room, bucketOptions.RoomSize)
	for i := uint64(0); i < b.bucketOptions.RoutineAmount; i++ {
		c := make(chan *proto.PushRoomMsgRequest, bucketOptions.RoutineSize)
		b.routines[i] = c
		go b.PushRoom(c)
	}
	return
}

// 通过负载均衡（数组）的方式接受异步消息
func (b *Bucket) PushRoom(ch chan *proto.PushRoomMsgRequest) {
	for {
		var (
			arg  *proto.PushRoomMsgRequest
			room *Room
		)
		//阻塞，接收广播消息
		arg = <-ch

		// 先获取该Bucket上存在的room，如果有，就把消息广播到此room
		if room = b.Room(arg.RoomId); room != nil {
			room.Push(&arg.Msg)
		}
	}
}

// Room：获取Bucket上的room信息；如果没有，返回nil
func (b *Bucket) Room(rid int) (room *Room) {
	b.cLock.RLock()
	room, _ = b.rooms[rid]
	b.cLock.RUnlock()
	return
}

// Put：存储某个已经经过认证的用户（userid），关联room（roomid），关联会话（Channel）
func (b *Bucket) Put(userId int /*USERID：客户端唯一ID-整形*/, roomId int, ch *Channel) (err error) {
	var (
		room *Room
		ok   bool
	)
	b.cLock.Lock()
	if roomId != NoRoom {
		// roomid不为-1
		if room, ok = b.rooms[roomId]; !ok {
			// 如果room不存在，就新建一个
			room = NewRoom(roomId)
			b.rooms[roomId] = room
		}
		ch.Room = room
	}
	ch.userId = userId

	//chs存放 userid=>会话Channel的映射关系
	b.chs[userId] = ch
	b.cLock.Unlock()

	if room != nil {
		// 将Channel放入room
		// 将ch挂在room中，实际上是挂在双链表的尾部
		err = room.Put(ch)
	}
	return
}

// DeleteChannel：删除Bucket上某个Channel会话
func (b *Bucket) DeleteChannel(ch *Channel) {
	var (
		ok   bool
		room *Room
	)
	b.cLock.RLock()
	if ch, ok = b.chs[ch.userId]; ok {
		room = b.chs[ch.userId].Room
		//delete from bucket
		delete(b.chs, ch.userId)
	}
	if room != nil && room.DeleteChannel(ch) {
		// if room empty delete,will mark room.drop is true
		if room.drop == true {
			delete(b.rooms, room.Id)
		}
	}
	b.cLock.RUnlock()
}

func (b *Bucket) Channel(userId int) (ch *Channel) {
	b.cLock.RLock()
	ch = b.chs[userId]
	b.cLock.RUnlock()
	return
}

// 对于单个bucket：把pushRoomMsgReq房间信息异步放入队列，最终数据会被Bucket.PushRoom接收并处理
func (b *Bucket) BroadcastRoom(pushRoomMsgReq *proto.PushRoomMsgRequest) {
	num := atomic.AddUint64(&b.routinesNum, 1) % b.bucketOptions.RoutineAmount
	b.routines[num] <- pushRoomMsgReq
}
