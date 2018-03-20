package antidoteclient

import (
	"fmt"
	"gopkg.in/fatih/pool.v2"
	"math/rand"
	"net"
	"time"
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

func NewClient(hosts ...Host) (client *Client, err error) {
	pools := make([]pool.Pool, len(hosts))
	for i, h := range hosts {
		p, err := pool.NewChannelPool(INITIAL_POOL_SIZE, MAX_POOL_SIZE, func() (net.Conn, error) { return net.Dial("tcp", fmt.Sprintf("%s:%d", h.Name, h.Port)) })
		if err != nil {
			return nil, err
		}
		pools[i] = p
	}
	client = &Client{
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
	for _, i := range r.Perm(len(client.pools)) {
		p := client.pools[i]
		con, err := p.Get()
		if err != nil {
			return nil, err
		}
		c = &Connection{
			Conn: con,
			pool: p,
		}
		return c, nil
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
	txID []byte
	con  *Connection
}

func (client *Client) StartTransaction() (tx *InteractiveTransaction, err error) {
	con, err := client.getConnection()
	if err != nil {
		return
	}
	readwrite := uint32(0)
	blue := uint32(0)
	apbtxn := &ApbStartTransaction{
		Properties: &ApbTxnProperties{ReadWrite: &readwrite, RedBlue: &blue},
	}
	err = apbtxn.encode(con)
	if err != nil {
		return
	}

	apbtxnresp, err := decodeStartTransactionResp(con)
	if err != nil {
		return
	}
	txndesc := apbtxnresp.TransactionDescriptor
	tx = &InteractiveTransaction{
		con:  con,
		txID: txndesc,
	}
	return
}

func (tx *InteractiveTransaction) update(updates ...*ApbUpdateOp) (op *ApbOperationResp, err error) {
	apbUpdate := &ApbUpdateObjects{
		Updates:               updates,
		TransactionDescriptor: tx.txID,
	}
	err = apbUpdate.encode(tx.con)
	if err != nil {
		return
	}
	return decodeOperationResp(tx.con)
}

func (tx *InteractiveTransaction) read(objects ...*ApbBoundObject) (resp *ApbReadObjectsResp, err error) {
	apbUpdate := &ApbReadObjects{
		TransactionDescriptor: tx.txID,
		Boundobjects:          objects,
	}
	err = apbUpdate.encode(tx.con)
	if err != nil {
		return
	}
	return decodeReadObjectsResp(tx.con)
}

func (tx *InteractiveTransaction) commit() (op *ApbCommitResp, err error) {
	msg := &ApbCommitTransaction{}
	err = msg.encode(tx.con)
	if err != nil {
		return
	}
	return decodeCommitResp(tx.con)
}
