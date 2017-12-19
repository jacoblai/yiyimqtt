// Copyright (c) 2014 The gomqtt Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package broker

import (
	"sync"
	"time"
	"path/filepath"
	"os"
	"fmt"
	"tools"
	"github.com/jacoblai/yiyidb"
	"transport"
	"packet"
	"gopkg.in/tomb.v2"
)

// LogEvent are received by a Logger.
type LogEvent int

const (
	// NewConnection is emitted when a client comes online.
	NewConnection LogEvent = iota

	// PacketReceived is emitted when a packet has been received.
	PacketReceived

	// MessagePublished is emitted after a message has been published.
	MessagePublished

	// MessageForwarded is emitted after a message has been forwarded.
	MessageForwarded

	// PacketSent is emitted when a packet has been sent.
	PacketSent

	// LostConnection is emitted when the connection has been terminated.
	LostConnection

	// TransportError is emitted when an underlying transport error occurs.
	TransportError

	// SessionError is emitted when a call to the session fails.
	SessionError

	// BackendError is emitted when a call to the backend fails.
	BackendError

	// ClientError is emitted when the client violates the protocol.
	ClientError
)

// The Logger callback handles incoming log messages.
type Logger func(LogEvent, *Client, packet.Packet, *packet.Message, error)

// The Engine handles incoming connections and connects them to the backend.
type Engine struct {
	Logger  Logger

	ConnectTimeout   time.Duration
	DefaultReadLimit int64

	clients       map[string]*Client
	cmqtt         *tools.Tree
	retained      *yiyidb.Kvdb
	offlineData   *yiyidb.ChanQueue
	storein       *yiyidb.Kvdb
	storeout      *yiyidb.Kvdb
	subscriptions *yiyidb.Kvdb
	counter       *yiyidb.Kvdb
	wills         *yiyidb.Kvdb
	checker       *tools.Cp_checker

	closing   bool
	mutex     sync.Mutex
	waitGroup sync.WaitGroup

	tomb tomb.Tomb
}

func NewEngine() *Engine {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	dir = dir + "/data"
	if _, err := os.Stat(dir); err != nil {
		if err = os.MkdirAll(dir, 0755); err != nil {
			panic(err)
		}
	}

	eng := &Engine{
		cmqtt:    tools.NewTree(),
		clients:  make(map[string]*Client),
		checker:  tools.NewCpChecker(),
	}

	eng.subscriptions, err = yiyidb.OpenKvdb(dir+"/cp7subs", false, false, 10) //path, enable ttl
	if err != nil {
		panic(err)
	}

	eng.storein, err = yiyidb.OpenKvdb(dir+"/cp7storesin", false, false, 10) //path, enable ttl
	if err != nil {
		panic(err)
	}

	eng.storeout, err = yiyidb.OpenKvdb(dir+"/cp7storesout", false, false, 10) //path, enable ttl
	if err != nil {
		panic(err)
	}

	eng.counter, err = yiyidb.OpenKvdb(dir+"/cp7counter", false, false, 10) //path, enable ttl
	if err != nil {
		panic(err)
	}

	eng.retained, err = yiyidb.OpenKvdb(dir+"/cp7retained", false, false, 10) //path, enable ttl
	if err != nil {
		panic(err)
	}

	eng.offlineData, err = yiyidb.OpenChanQueue(dir+"/cp7ols", 10)
	if err != nil {
		panic(err)
	}

	eng.wills, err = yiyidb.OpenKvdb(dir+"/cp7wills", true, false, 10) //path, enable ttl
	if err != nil {
		panic(err)
	}

	return eng
}

// Accept begins accepting connections from the passed server.
func (e *Engine) Accept(server transport.Server) {
	e.tomb.Go(func() error {
		for {
			conn, err := server.Accept()
			if err != nil {
				return err
			}

			if !e.Handle(conn) {
				return nil
			}
		}
	})
}

// Handle takes over responsibility and handles a transport.Conn. It returns
// false if the engine is closing and the connection has been closed.
func (e *Engine) Handle(conn transport.Conn) bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// check conn
	if conn == nil {
		panic("passed conn is nil")
	}

	// set default read limit
	conn.SetReadLimit(e.DefaultReadLimit)

	// close conn immediately when closing
	if e.closing {
		conn.Close()
		return false
	}

	// handle client
	newClient(e, conn)

	return true
}

func (e *Engine) Reset(clientid string) {
	e.CounterReset(clientid)
	e.storein.Clear(clientid + "-")
	e.storeout.Clear(clientid + "-")
	e.subscriptions.Clear(clientid + "-")
	e.wills.Clear(clientid)
	e.offlineData.Clear(clientid)
}

// Close will stop handling incoming connections and close all current clients.
// The call will block until all clients are properly closed.
//
// Note: All passed servers to Accept must be closed before calling this method.
func (e *Engine) Close() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.subscriptions.Close()
	e.offlineData.Close()
	e.storein.Close()
	e.storeout.Close()
	e.wills.Close()
	e.counter.Close()

	// set closing
	e.closing = true

	// stop acceptors
	e.tomb.Kill(nil)
	e.tomb.Wait()

	// close all clients
	for _, client := range e.clients {
		client.Close(false)
	}
}

// Wait can be called after close to wait until all clients have been closed.
// The method returns whether all clients have been closed (true) or the timeout
// has been reached (false).
func (e *Engine) Wait(timeout time.Duration) bool {
	wait := make(chan struct{})

	go func() {
		e.waitGroup.Wait()
		close(wait)
	}()

	select {
	case <-wait:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (e *Engine) Terminate(c *Client) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// clear all subscriptions
	e.cmqtt.Clear(c)

	cid := c.ClientID()
	// remove client from list
	_, ok := e.clients[cid]
	if ok {
		delete(e.clients, cid)
	}

	//send will
	e.SendWill(c.ClientID())

	return nil
}

func (e *Engine) Subscribe(c *Client, subs *packet.Subscription, reuse bool) error {
	_, err := e.checker.CheckSubTopic([]byte(subs.Topic))
	if err != nil {
		return err
	}
	// add subscription
	e.cmqtt.Add(subs.Topic, c)

	cid := c.ClientID()
	if !reuse && subs.QOS >= 1 {
		err = e.SaveSubscription(cid, subs)
		if err != nil {
			return err
		}
	} else if subs.QOS == 0 {
		err = e.DeleteSubscription(cid, subs.Topic)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) Unsubscribe(c *Client, subs *packet.UnsubscribePacket) error {
	for _, topic := range subs.Topics {
		// remove subscription
		e.cmqtt.Remove(topic, c)

		// remove subscription from db
		e.DeleteSubscription(c.ClientID(), topic)
	}

	return nil
}

func (e *Engine) Publish(msg *packet.Message) error {
	err := e.checker.CheckPubTopic([]byte(msg.Topic))
	if err != nil {
		println(err.Error())
		return err
	}

	// publish directly to clients
	for _, v := range e.cmqtt.Match(msg.Topic) {
		v.(*Client).Publish(msg)
	}

	// otherwise get stored subscriptions
	subs := e.LookupSubscriptionsByTopic(msg.Topic)
	if err != nil {
		return err
	}
	// iterate through stored subscriptions
	for _, sub := range subs {
		if sub.Sub.QOS >= 1 {
			e.offlineData.EnqueueObject(sub.ClientId, msg)
		}
	}

	return nil
}

func (e *Engine) Authenticate(client *Client, clientid, user, password string) (bool, error) {
	fmt.Println(clientid, len(e.clients))
	return true, nil
}

func (e *Engine) Setup(c *Client, pkt *packet.ConnectPacket) (bool, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	cid := c.ClientID()
	// return a new temporary session if id is zero
	if len(cid) == 0 {
		return false, nil
	}

	// close existing client
	existingClient, ok := e.clients[cid]
	if ok {
		existingClient.Close(true)
	}

	// add new client
	e.clients[cid] = c

	// when found
	if ok {
		return true, nil
	}

	// reset the session if clean is requested
	if pkt.CleanSession {
		e.Reset(cid)
	}

	// save will if present
	if pkt.Will != nil {
		e.StoreWill(cid,*pkt.Will.Copy())
	}

	return false, nil
}

func (e *Engine) PassQosPackets(c *Client, pkt *packet.ConnectPacket) error {
	cid := c.ClientID()
	// retrieve stored packets
	packets, err := e.AllPackets(cid, outgoing)
	if err != nil {
		return err
	}

	// resend stored packets
	for _, opkt := range packets {
		if opkt.Type() == packet.PUBLISH {
			opkt.(*packet.PublishPacket).Dup = true
		}
		// send connack
		err = c.send(opkt, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) PassSubs(c *Client, pkt *packet.ConnectPacket) error {
	// attempt to restore client if not clean
	if !pkt.CleanSession {
		cid := c.ClientID()
		// get stored subscriptions
		subs, err := e.AllSubscriptions(cid)
		if err != nil {
			return err
		}

		// resubscribe subscriptions
		for _, sub := range subs {
			err = e.Subscribe(c, sub, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) PassOfflineMsg(c *Client, pkt *packet.ConnectPacket) error {
	// attempt to restore client if not clean
	if !pkt.CleanSession {
		cid := c.ClientID()
		for {
			//send offline msg
			vals, err := e.offlineData.Dequeue(cid)
			if err != nil {
				break
			}
			remsg := &packet.Message{}
			err = vals.ToObject(remsg)
			if err == nil {
				res := c.Publish(remsg)
				if !res {
					break
				}
			}
		}

	}

	return nil
}