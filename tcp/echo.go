package tcp

import (
	"bufio"
	"context"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoHandler echos received line to client, using for test
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

// MakeEchoHandler creates EchoHandler
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient is client for EchoHandler, using for test
type EchoClient struct {
	conn net.Conn
	wait wait.Wait
}

func (c *EchoClient) Close() error {
	c.wait.WaitWithTimeout(time.Second * 10)
	_ = c.conn.Close()
	return nil
}

func (h *EchoHandler) Handle(_ context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}
	client := &EchoClient{
		conn: conn,
	}
	h.activeConn.Store(client, struct {}{})

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.wait.Add(1)
		// 模拟关闭时未完成发送的情况
		//logger.Info("sleeping")
		//time.Sleep(10 * time.Second)
		bytes := []byte(msg)
		_, _ = conn.Write(bytes)
		// 发送完毕, 结束waiting
		client.wait.Done()
	}
}

// Close stops echo handler
// 关闭服务器
func (h *EchoHandler) Close() error {
	logger.Info("handle shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}