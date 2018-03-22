package antidoteclient

import (
	"bytes"
	"fmt"
)

type Bucket struct {
	Bucket []byte
}

type Transaction interface {
	Read(objects ...*ApbBoundObject) (resp *ApbReadObjectsResp, err error)
	Update(updates ...*ApbUpdateOp) (op *ApbOperationResp, err error)
}

type Key []byte

type CRDTReader interface {
	ReadSet(tx Transaction, key Key) (val [][]byte, err error)
	ReadReg(tx Transaction, key Key) (val []byte, err error)
	ReadMap(tx Transaction, key Key) (val *MapReadResult, err error)
	ReadMVReg(tx Transaction, key Key) (val [][]byte, err error)
	ReadCounter(tx Transaction, key Key) (val int32, err error)
}

type InteractiveTransaction struct {
	txID []byte
	con  *Connection
	commited bool
}

func (tx *InteractiveTransaction) Update(updates ...*ApbUpdateOp) (op *ApbOperationResp, err error) {
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

func (tx *InteractiveTransaction) Read(objects ...*ApbBoundObject) (resp *ApbReadObjectsResp, err error) {
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

func (tx *InteractiveTransaction) Commit() (op *ApbCommitResp, err error) {
	msg := &ApbCommitTransaction{TransactionDescriptor: tx.txID}
	err = msg.encode(tx.con)
	if err != nil {
		return
	}
	op, err = decodeCommitResp(tx.con)
	tx.con.Close()
	return
}

func (tx *InteractiveTransaction) Abort() (op *ApbOperationResp, err error) {
	msg := &ApbAbortTransaction{TransactionDescriptor: tx.txID}
	err = msg.encode(tx.con)
	if err != nil {
		return
	}
	op, err = decodeOperationResp(tx.con)
	tx.con.Close()
	return
}



type StaticTransaction struct {
	client *Client
}

func (tx *StaticTransaction) Update(updates ...*ApbUpdateOp) (op *ApbOperationResp, err error) {
	apbStaticUpdate := &ApbStaticUpdateObjects{
		Transaction: &ApbStartTransaction{Properties: &ApbTxnProperties{}},
		Updates: updates,
	}
	con, err := tx.client.getConnection()
	if err != nil {
		return
	}
	err = apbStaticUpdate.encode(con)
	if err != nil {
		return
	}
	resp, err := decodeCommitResp(con)
	con.Close()
	if err != nil {
		return
	}
	return &ApbOperationResp{Success: resp.Success, Errorcode: resp.Errorcode}, nil
}


func (tx *StaticTransaction) Read(objects ...*ApbBoundObject) (resp *ApbReadObjectsResp, err error) {
	apbRead := &ApbStaticReadObjects{
		Transaction: &ApbStartTransaction{Properties: &ApbTxnProperties{}},
		Objects: objects,
	}
	con, err := tx.client.getConnection()
	if err != nil {
		return
	}
	err = apbRead.encode(con)
	if err != nil {
		return
	}
	sresp, err := decodeStaticReadObjectsResp(con)
	con.Close()
	if err != nil {
		return
	}
	return sresp.Objects, nil
}


func (bucket *Bucket) ReadSet(tx Transaction, key Key) (val [][]byte, err error) {
	crdtType := CRDTType_ORSET
	resp, err := tx.Read(&ApbBoundObject{Bucket: bucket.Bucket, Key: key, Type: &crdtType})
	if err != nil {
		return
	}
	val = resp.Objects[0].Set.Value
	return
}

func (bucket *Bucket) ReadReg(tx Transaction, key Key) (val []byte, err error) {
	crdtType := CRDTType_LWWREG
	resp, err := tx.Read(&ApbBoundObject{Bucket: bucket.Bucket, Key: key, Type: &crdtType})
	if err != nil {
		return
	}
	val = resp.Objects[0].Reg.Value
	return
}

func (bucket *Bucket) ReadMap(tx Transaction, key Key) (val *MapReadResult, err error) {
	crdtType := CRDTType_AWMAP
	resp, err := tx.Read(&ApbBoundObject{Bucket: bucket.Bucket, Key: key, Type: &crdtType})
	if err != nil {
		return
	}
	val = &MapReadResult{mapResp: resp.Objects[0].Map}
	return
}

func (bucket *Bucket) ReadMVReg(tx Transaction, key Key) (val [][]byte, err error) {
	crdtType := CRDTType_MVREG
	resp, err := tx.Read(&ApbBoundObject{Bucket: bucket.Bucket, Key: key, Type: &crdtType})
	if err != nil {
		return
	}
	val = resp.Objects[0].Mvreg.Values
	return
}

func (bucket *Bucket) ReadCounter(tx Transaction, key Key) (val int32, err error) {
	crdtType := CRDTType_COUNTER
	resp, err := tx.Read(&ApbBoundObject{Bucket: bucket.Bucket, Key: key, Type: &crdtType})
	if err != nil {
		return
	}
	val = *resp.Objects[0].Counter.Value
	return
}

func (mrr *MapReadResult) Set(key Key) (val [][]byte, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_ORSET && bytes.Equal(me.Key.Key, key) {
			return me.Value.Set.Value, nil
		}
	}
	return nil, fmt.Errorf("set entry with key '%s' not found", key)
}



type MapReadResult struct {
	mapResp *ApbGetMapResp
}

func (mrr *MapReadResult) Reg(key Key) (val []byte, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_LWWREG && bytes.Equal(me.Key.Key, key) {
			return me.Value.Reg.Value, nil
		}
	}
	return nil, fmt.Errorf("register entry with key '%s' not found", key)
}

func (mrr *MapReadResult) Map(key Key) (val *MapReadResult, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_AWMAP && bytes.Equal(me.Key.Key, key) {
			return &MapReadResult{mapResp: me.Value.Map}, nil
		}
	}
	return nil, fmt.Errorf("map entry with key '%s' not found", key)
}

func (mrr *MapReadResult) MVReg(key Key) (val [][]byte, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_MVREG && bytes.Equal(me.Key.Key, key) {
			return me.Value.Mvreg.Values, nil
		}
	}
	return nil, fmt.Errorf("map entry with key '%s' not found", key)
}

func (mrr *MapReadResult) Counter(key Key) (val int32, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_COUNTER && bytes.Equal(me.Key.Key, key) {
			return *me.Value.Counter.Value, nil
		}
	}
	return 0, fmt.Errorf("counter entry with key '%s' not found", key)
}

// Updates
type UpdateConverter interface {
	ConvertToToplevel(bucket []byte) *ApbUpdateOp
	ConvertToNested() *ApbMapNestedUpdate
}

type CRDTUpdate struct {
	Update *ApbUpdateOperation
	Key    []byte
	Type   CRDTType
}

type CRDTUpdater interface {
	Update(tx Transaction, updates... *CRDTUpdate) (resp *ApbOperationResp, err error)
}

func (bucket *Bucket) Update(tx Transaction, updates... *CRDTUpdate) (resp *ApbOperationResp, err error) {
	updateOps := make([]*ApbUpdateOp, len(updates))
	for i,v := range updates {
		updateOps[i] = v.ConvertToToplevel(bucket.Bucket)
	}
	return tx.Update(updateOps...)
}

func (update *CRDTUpdate) ConvertToToplevel(bucket []byte) *ApbUpdateOp {
	return &ApbUpdateOp{
		Boundobject: &ApbBoundObject{Key: update.Key, Type: &update.Type, Bucket: bucket},
		Operation:   update.Update,
	}
}

func (update *CRDTUpdate) ConvertToNested() *ApbMapNestedUpdate {
	return &ApbMapNestedUpdate{
		Key:    &ApbMapKey{Key: update.Key, Type: &update.Type},
		Update: update.Update,
	}
}

// CRDT update operations

func SetAdd(key Key, elems ... []byte) *CRDTUpdate {
	optype := ApbSetUpdate_ADD
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_ORSET,
		Update: &ApbUpdateOperation{
			Setop: &ApbSetUpdate{Adds: elems, Optype: &optype},
		},
	}
}

func SetRemove(key Key, elems ... []byte) *CRDTUpdate {
	optype := ApbSetUpdate_REMOVE
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_ORSET,
		Update: &ApbUpdateOperation{
			Setop: &ApbSetUpdate{Adds: elems, Optype: &optype},
		},
	}
}

func CounterInc(key Key, inc int64) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_COUNTER,
		Update: &ApbUpdateOperation{
			Counterop: &ApbCounterUpdate{Inc: &inc},
		},
	}
}

func IntegerInc(key Key, inc int64) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_ORSET,
		Update: &ApbUpdateOperation{
			Integerop: &ApbIntegerUpdate{Inc: &inc},
		},
	}
}

func IntegerSet(key Key, value int64) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_ORSET,
		Update: &ApbUpdateOperation{
			Integerop: &ApbIntegerUpdate{Set: &value},
		},
	}
}

func RegPut(key Key, value []byte) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_LWWREG,
		Update: &ApbUpdateOperation{
			Regop: &ApbRegUpdate{Value: value},
		},
	}
}

func MVRegPut(key Key, value []byte) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_MVREG,
		Update: &ApbUpdateOperation{
			Regop: &ApbRegUpdate{Value: value},
		},
	}
}

func MapUpdate(key Key, updates ... *CRDTUpdate) *CRDTUpdate {
	nupdates := make([]*ApbMapNestedUpdate, len(updates))
	for i,v := range updates {
		nupdates[i] = v.ConvertToNested()
	}
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_AWMAP,
		Update: &ApbUpdateOperation{
			Mapop: &ApbMapUpdate{Updates: nupdates},
		},
	}
}
