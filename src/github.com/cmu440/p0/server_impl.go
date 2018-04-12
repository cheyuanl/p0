/**
 * This key-value storage server handles multiple cliMngPool concurrently and broadcasts
 * the response to cliMngPool. The sychronizations are handled by golang built-in channels.
 *
 * The message flow works as follows:
 * 1. Fan-in stage
 * 	 Multiple clients produce requests to the cliReqChan.
 *   The cliReqChan is consumed by UpdateKvstore routine that runs in single thread to ensure
 *   serial access of hashtable.
 * 2. Fan-out stage
 *   UpdateKvstore produces the msg to the broadcastChan, which is consumed by Broatcast routine
 *   that send response to the minBoxChans of cliMngPool.
 *
 * @author Che-Yuan Liang Apr. 2018
 * Issue: channel deadlock when producer is done and waiting for consumer.
 */

package p0

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	inboxBuffSize = 500
	putOp         = "put"
	getOp         = "get"
	addCli        = "addCli"
	delCli        = "delCli"
)

type kvPair struct {
	key   string
	value []byte
}

type client struct {
	conn       net.Conn
	inBox      chan []byte // the message queue to be consumed by client
	readerChan chan []byte
}

// The client request event
type cliReq struct {
	op  string
	kvp *kvPair
}

// Concurrent client connection management
type cliMng struct {
	op  string
	cli *client
}

type cliManager struct {
	clients    map[*client]bool // keep track of the connections
	mangChan   chan cliMng      // cahnnel for client managment events
	cliReqChan chan bool
	cliGetChan chan []*client
	done       chan bool
}

// This is responsible for managing the connected clients
func NewCliManager() *cliManager {
	return &cliManager{
		clients:    make(map[*client]bool),
		mangChan:   make(chan cliMng),
		cliReqChan: make(chan bool),
		cliGetChan: make(chan []*client),
		done:       make(chan bool)}
}

func (cm *cliManager) Start() {
	for {
		select {
		case mng := <-cm.mangChan:
			switch mng.op {
			case addCli:
				cm.clients[mng.cli] = true
			case delCli:
				if _, ok := cm.clients[mng.cli]; ok {
					delete(cm.clients, mng.cli)
					mng.cli.conn.Close()
				}
			default:
				panic("something wrong.")
			}

		case <-cm.cliReqChan:
			keys := make([]*client, 0, len(cm.clients))
			for cli := range cm.clients {
				keys = append(keys, cli)
			}
			cm.cliGetChan <- keys

		case <-cm.done:
			for cli := range cm.clients {
				delete(cm.clients, cli)
				cli.conn.Close()
			}
			return
		}
	}
}

func (cm *cliManager) Close() {
	cm.done <- true
}

func (cm *cliManager) Add(cli *client) {
	cm.mangChan <- cliMng{addCli, cli}
}

func (cm *cliManager) Delete(cli *client) {
	cm.mangChan <- cliMng{delCli, cli}
}

func (cm *cliManager) GetClients() []*client {
	cm.cliReqChan <- true
	return <-cm.cliGetChan
}

type keyValueServer struct {
	listener      net.Listener
	cliManager    *cliManager
	cliReqChan    chan cliReq // channel for client request events
	broadcastChan chan []byte // channel for message to be broadcasted
	updateDone    chan bool
	broadcDone    chan bool
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{
		cliManager:    NewCliManager(),
		cliReqChan:    make(chan cliReq),
		broadcastChan: make(chan []byte),
		updateDone:    make(chan bool),
		broadcDone:    make(chan bool),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// create a listener
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if ln == nil {
		return err
	}

	// init
	kvs.listener = ln
	init_db()

	// spawn routines
	go kvs.AcceptClients()

	// fan-in
	go kvs.cliManager.Start()

	go kvs.UpdateKvStore()

	// fan-out
	go kvs.Broadcast()

	return err
}

// shutdown all go routines and the server
func (kvs *keyValueServer) Close() {
	// destroy listener
	kvs.listener.Close()
	// destroy all client connections
	kvs.cliManager.Close()
	// destroy all other go routines
	kvs.updateDone <- true
	kvs.broadcDone <- true
}

func (kvs *keyValueServer) Count() int {
	return len(kvs.cliManager.GetClients())
}

func (kvs *keyValueServer) ReadClientConn(cli *client) {
	defer kvs.cliManager.Delete(cli)

	reader := bufio.NewReader(cli.conn)
	for {
		// read bytes
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}

		// parse request
		line := strings.Split(strings.TrimSuffix(string(msg), "\n"), ",")
		if len(line) < 2 {
			fmt.Println("Invalid request sent from", cli.conn)
			return
		}
		op := line[0]
		key := line[1]

		// produce cliReq event
		switch op {
		case putOp:
			if len(line) != 3 {
				fmt.Println("Invalid request sent from", cli.conn)
				return
			}
			value := []byte(line[2])
			kvs.cliReqChan <- cliReq{putOp, &kvPair{key, value}}
		case getOp:
			kvs.cliReqChan <- cliReq{getOp, &kvPair{key, nil}}
		default:
			fmt.Println("Invalid request", cli.conn)
			return
		}
	}
}

func (kvs *keyValueServer) WriteClientConn(cli *client) {
	defer kvs.cliManager.Delete(cli)

	for {
		msg := <-cli.inBox
		if _, err := cli.conn.Write(msg); err != nil {
			return
		}
	}
}

// Consume the messages from broadcastChan and produce it to each inbox.
func (kvs *keyValueServer) Broadcast() {
	for {
		select {
		case msg := <-kvs.broadcastChan:
			for _, cli := range kvs.cliManager.GetClients() {
				if cli != nil && len(cli.inBox) < inboxBuffSize {
					cli.inBox <- append([]byte(msg), []byte("\n")...)
				}
				// drop the msg if the inbox is full
			}
		case <-kvs.broadcDone:
			return
		}
	}
}

func (kvs *keyValueServer) AcceptClients() {
	if kvs.listener == nil {
		panic("listener is nil")
	}

	for {
		// This blocking call will return err went kvs.Close() is called
		conn, err := kvs.listener.Accept()
		if err != nil {
			return
		}

		cli := &client{conn: conn,
			inBox:      make(chan []byte, inboxBuffSize),
			readerChan: make(chan []byte),
		}
		kvs.cliManager.Add(cli)
		go kvs.ReadClientConn(cli)
		go kvs.WriteClientConn(cli)
	}

}

func (kvs *keyValueServer) UpdateKvStore() {
	for {
		select {
		case <-kvs.updateDone:
			return
		case cliReq := <-kvs.cliReqChan:
			switch cliReq.op {
			case putOp:
				put(cliReq.kvp.key, cliReq.kvp.value)
			case getOp:
				value := get(cliReq.kvp.key)
				kvs.broadcastChan <- bytes.Join([][]byte{[]byte(cliReq.kvp.key), value}, []byte(","))
			default:
				panic("something when wrong with cliReq producer")
			}
		}
	}
}
