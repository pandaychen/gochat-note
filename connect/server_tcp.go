/**
 * Created by lock
 * Date: 2020/4/14
 */
package connect

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"gochat/api/rpc"
	"gochat/config"
	"gochat/pkg/stickpackage"
	"gochat/proto"
	"net"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const maxInt = 1<<31 - 1

func init() {
	rpc.InitLogicRpcClient()
}

func (c *Connect) InitTcpServer() error {
	aTcpAddr := strings.Split(config.Conf.Connect.ConnectTcp.Bind, ",")
	cpuNum := config.Conf.Connect.ConnectBucket.CpuNum
	var (
		addr     *net.TCPAddr
		listener *net.TCPListener
		err      error
	)
	for _, ipPort := range aTcpAddr {
		if addr, err = net.ResolveTCPAddr("tcp", ipPort); err != nil {
			logrus.Errorf("server_tcp ResolveTCPAddr error:%s", err.Error())
			return err
		}
		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			logrus.Errorf("net.ListenTCP(tcp, %s),error(%v)", ipPort, err)
			return err
		}
		logrus.Infof("start tcp listen at:%s", ipPort)
		// cpu core num
		for i := 0; i < cpuNum; i++ {
			go c.acceptTcp(listener)
		}
	}
	return nil
}

func (c *Connect) acceptTcp(listener *net.TCPListener) {
	var (
		conn *net.TCPConn
		err  error
		r    int
	)
	connectTcpConfig := config.Conf.Connect.ConnectTcp
	for {
		if conn, err = listener.AcceptTCP(); err != nil {
			logrus.Errorf("listener.Accept(\"%s\") error(%v)", listener.Addr().String(), err)
			return
		}
		// set keep alive，client==server ping package check
		if err = conn.SetKeepAlive(connectTcpConfig.KeepAlive); err != nil {
			logrus.Errorf("conn.SetKeepAlive() error:%s", err.Error())
			return
		}
		//set ReceiveBuf
		if err := conn.SetReadBuffer(connectTcpConfig.ReceiveBuf); err != nil {
			logrus.Errorf("conn.SetReadBuffer() error:%s", err.Error())
			return
		}
		//set SendBuf
		if err := conn.SetWriteBuffer(connectTcpConfig.SendBuf); err != nil {
			logrus.Errorf("conn.SetWriteBuffer() error:%s", err.Error())
			return
		}
		go c.ServeTcp(DefaultServer, conn, r)
		if r++; r == maxInt {
			logrus.Infof("conn.acceptTcp num is:%d", r)
			r = 0
		}
	}
}

// ServeTcp：处理业务逻辑（新连接）
func (c *Connect) ServeTcp(server *Server, conn *net.TCPConn, r int) {
	var ch *Channel
	// 接收一个新连接，需要新建一个新的会话，然后把会话放在room中
	ch = NewChannel(server.Options.BroadcastSize)
	ch.connTcp = conn
	// 针对每个TCP连接，都开启两个独立的goroutine，一个处理读、另一个处理写
	go c.writeDataToTcp(server, ch)
	go c.readDataFromTcp(server, ch)
}

// readDataFromTcp：接收协议并处理
// 注意，这里为什么传入ch *Channel？因为要根据用户发送的数据来修改ch的值，比如ch加入哪个room？哪个bucket？
// 可借鉴：收包处理
func (c *Connect) readDataFromTcp(s *Server, ch *Channel) {
	defer func() {
		logrus.Infof("start exec disConnect ...")
		if ch.Room == nil || ch.userId == 0 {
			logrus.Infof("roomId and userId eq 0")
			_ = ch.connTcp.Close()
			return
		}
		logrus.Infof("exec disConnect ...")
		disConnectRequest := new(proto.DisConnectRequest)
		disConnectRequest.RoomId = ch.Room.Id
		disConnectRequest.UserId = ch.userId
		s.Bucket(ch.userId).DeleteChannel(ch)
		if err := s.operator.DisConnect(disConnectRequest); err != nil {
			logrus.Warnf("DisConnect rpc err :%s", err.Error())
		}
		if err := ch.connTcp.Close(); err != nil {
			logrus.Warnf("DisConnect close tcp conn err :%s", err.Error())
		}
		return
	}()
	// scanner
	scannerPackage := bufio.NewScanner(ch.connTcp)
	scannerPackage.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if !atEOF && data[0] == 'v' {
			if len(data) > stickpackage.TcpHeaderLength {
				packSumLength := int16(0)
				_ = binary.Read(bytes.NewReader(data[stickpackage.LengthStartIndex:stickpackage.LengthStopIndex]), binary.BigEndian, &packSumLength)
				if int(packSumLength) <= len(data) {
					return int(packSumLength), data[:packSumLength], nil
				}
			}
		}
		return
	})
	scanTimes := 0
	for {
		scanTimes++
		if scanTimes > 3 {
			logrus.Infof("scannedPack times is:%d", scanTimes)
			break
		}
		for scannerPackage.Scan() {
			scannedPack := new(stickpackage.StickPackage)
			err := scannedPack.Unpack(bytes.NewReader(scannerPackage.Bytes()))
			if err != nil {
				logrus.Errorf("scan tcp package err:%s", err.Error())
				break
			}
			//get a full package
			var connReq proto.ConnectRequest
			logrus.Infof("get a tcp message :%s", scannedPack)
			var rawTcpMsg proto.SendTcp
			if err := json.Unmarshal([]byte(scannedPack.Msg), &rawTcpMsg); err != nil {
				logrus.Errorf("tcp message struct %+v", rawTcpMsg)
				break
			}
			logrus.Infof("json unmarshal,raw tcp msg is:%+v", rawTcpMsg)
			if rawTcpMsg.AuthToken == "" {
				logrus.Errorf("tcp s.operator.Connect no authToken")
				return
			}
			if rawTcpMsg.RoomId <= 0 {
				logrus.Errorf("tcp roomId not allow lgt 0")
				return
			}
			// tcp协议处理
			switch rawTcpMsg.Op {
			case config.OpBuildTcpConn:
				// 处理客户端新建连接的协议包
				connReq.AuthToken = rawTcpMsg.AuthToken
				connReq.RoomId = rawTcpMsg.RoomId //客户端希望连接到哪个room
				//fix
				//connReq.ServerId = config.Conf.Connect.ConnectTcp.ServerId
				connReq.ServerId = c.ServerId

				//调用RpcConnect.Connec方法，获取用户id
				userId, err := s.operator.Connect(&connReq)
				logrus.Infof("tcp s.operator.Connect userId is :%d", userId)
				if err != nil {
					logrus.Errorf("tcp s.operator.Connect error %s", err.Error())
					return
				}
				if userId == 0 {
					logrus.Error("tcp Invalid AuthToken ,userId empty")
					return
				}

				//按照用户id（userID）获取到响应的Bucket数组下标
				b := s.Bucket(userId)
				//insert into a bucket

				//把此connection(channel)放在此bucket中
				//1. 将此客户端放在bucket.rooms中，如果没有就新建一个，唯一的room_id
				//2. 将ch(channel)挂在上面的room中
				err = b.Put(userId, connReq.RoomId, ch)
				if err != nil {
					logrus.Errorf("tcp conn put room err: %s", err.Error())
					_ = ch.connTcp.Close()
					return
				}
			case config.OpRoomSend:
				//send tcp msg to room
				// 发送向房间内广播的消息
				req := &proto.Send{
					Msg:          rawTcpMsg.Msg,
					FromUserId:   rawTcpMsg.FromUserId,
					FromUserName: rawTcpMsg.FromUserName,
					RoomId:       rawTcpMsg.RoomId, //待广播的roomid
					Op:           config.OpRoomSend,
				}
				code, msg := rpc.RpcLogicObj.PushRoom(req)
				logrus.Infof("tcp conn push msg to room,err code is:%d,err msg is:%s", code, msg)
			}
		}
		if err := scannerPackage.Err(); err != nil {
			logrus.Errorf("tcp get a err package:%s", err.Error())
			return
		}
	}
}

// writeDataToTcp
func (c *Connect) writeDataToTcp(s *Server, ch *Channel) {
	//ping time default 54s
	ticker := time.NewTicker(DefaultServer.Options.PingPeriod)
	defer func() {
		ticker.Stop()
		_ = ch.connTcp.Close()
		return
	}()
	pack := stickpackage.StickPackage{
		Version: stickpackage.VersionContent,
	}
	for {
		select {
		case message, ok := <-ch.broadcast:
			if !ok {
				_ = ch.connTcp.Close()
				return
			}
			pack.Msg = message.Body
			pack.Length = pack.GetPackageLength()
			//send msg
			logrus.Infof("send tcp msg to conn:%s", pack.String())
			if err := pack.Pack(ch.connTcp); err != nil {
				logrus.Errorf("connTcp.write message err:%s", err.Error())
				return
			}
		case <-ticker.C:
			logrus.Infof("connTcp.ping message,send")
			//send a ping msg ,if error , return
			pack.Msg = []byte("ping msg")
			pack.Length = pack.GetPackageLength()
			if err := pack.Pack(ch.connTcp); err != nil {
				//send ping msg to tcp conn
				return
			}
		}
	}
}
