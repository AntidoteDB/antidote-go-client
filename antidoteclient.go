package antidoteclient

import (
	"gopkg.in/fatih/pool.v2"
	"net"
	"fmt"
	"math/rand"
	"time"
	"io"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
)

const INITIAL_POOL_SIZE = 1
const MAX_POOL_SIZE = 50

type Client struct {
	pools []pool.Pool
}

type Host struct {
	Name string
	Port int
}

func NewClient(hosts []Host) (client *Client, err error) {
	pools := make([]pool.Pool, len(hosts))
	for i, h := range hosts {
		p, err := pool.NewChannelPool(INITIAL_POOL_SIZE, MAX_POOL_SIZE, func () (net.Conn, error) { return net.Dial("tcp", fmt.Sprint("{}:{}", h.Name, h.Port)) })
		if err != nil {
			return
		}
		pools[i] = p
	}
	client = &Client {
		pools: pools,
	}
	return
}

func (client *Client) Close() {
	for _, p := range client.pools {
		p.Close()
	}
}

func (client *Client) getConnection() (c *Connection, err error) {
	// maybe make this global?
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, i :=  range r.Perm(len(client.pools)) {
		p := client.pools[i]
		con, err := p.Get()
		if err != nil {
			return
		}
		c = &Connection{
			Conn: con,
			pool: p,
		}
		return
	}
	err = fmt.Errorf("All connections dead")
	return
}

// a close already puts the connection back into the right pool
type Connection struct {
	net.Conn
	pool pool.Pool
}

type InteractiveTransaction struct {
	TxID []byte
	con Connection
}

func (op *ApbStartTransaction) encode(writer io.Writer) (err error) {
	msg, err := proto.Marshal(op)
	if err != nil {
		return
	}
	msgsize := len(msg)
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf[0:4], uint32(msgsize))
	buf[5] = 119
	writer.Write(buf)
	writer.Write(msg)
	return nil
}

func (client *Client) StartTransaction() (tx *InteractiveTransaction, err error) {
	con, err := client.getConnection()
	if err != nil {
		return
	}
	apbtxn := &ApbStartTransaction{
		Properties: &ApbTxnProperties{},
	}
	apbtxn.encode(con)
	// TODO get response, extract txid, return object
}
