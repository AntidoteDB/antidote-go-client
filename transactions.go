package antidoteclient

import (
	"bytes"
	"fmt"
)

// Represents a bucket in the Antidote database.
// Offers a high-level interface to issue read and write operations on objects in the bucket.
type Bucket struct {
	Bucket []byte
}

// A transaction object offers low-level mechanisms to send protocol-buffer messages to Antidote in the context of
// a highly-available transaction.
// Typical representatives are interactive transactions handled by Antidote and static transactions handled on the client side.
type Transaction interface {
	Read(objects ...*ApbBoundObject) (resp *ApbReadObjectsResp, err error)
	Update(updates ...*ApbUpdateOp) error
}

// Type alias for byte-slices.
// Used to represent keys of objects in buckets and maps
type Key []byte

// A CRDTReader allows to read the value of objects identified by keys in the context of a transaction.
type CRDTReader interface {
	// Read the value of a add-wins set identified by the given key
	ReadSet(tx Transaction, key Key) (val [][]byte, err error)
	// Read the value of a last-writer-wins register identified by the given key
	ReadReg(tx Transaction, key Key) (val []byte, err error)
	// Read the value of a add-wins map identified by the given key
	ReadMap(tx Transaction, key Key) (val *MapReadResult, err error)
	// Read the value of a multi-value register identified by the given key
	ReadMVReg(tx Transaction, key Key) (val [][]byte, err error)
	// Read the value of a counter identified by the given key
	ReadCounter(tx Transaction, key Key) (val int32, err error)
}

// A transaction handled by Antidote on the server side.
// Interactive Transactions need to be started on the server and are kept open for their duration.
// Update operations are only visible to reads issued in the context of the same transaction or after committing the transaction.
// Always commit or abort interactive transactions to clean up the server side!
type InteractiveTransaction struct {
	txID      []byte
	con       *connection
	committed bool
}

func (tx *InteractiveTransaction) Update(updates ...*ApbUpdateOp) error {
	apbUpdate := &ApbUpdateObjects{
		Updates:               updates,
		TransactionDescriptor: tx.txID,
	}
	err := apbUpdate.encode(tx.con)
	if err != nil {
		return err
	}
	resp, err := decodeOperationResp(tx.con)
	if err != nil {
		return err
	}
	if !(*resp.Success) {
		return fmt.Errorf("operation not successful; error code %d", *resp.Errorcode)
	}
	return nil
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

// commits the transaction, makes the updates issued under this transaction visible to subsequent transaction
// and cleans up the server side.
func (tx *InteractiveTransaction) Commit() error {
	if !tx.committed {
		msg := &ApbCommitTransaction{TransactionDescriptor: tx.txID}
		err := msg.encode(tx.con)
		if err != nil {
			return err
		}
		op, err := decodeCommitResp(tx.con)
		tx.con.Close()
		if err != nil {
			return err
		}
		if !(*op.Success) {
			return fmt.Errorf("operation not successful; error code %d", *op.Errorcode)
		}
	}
	return nil
}

// aborts the transactions, discards updates issued under this transaction
// and cleans up the server side.
// WARNING: May not be supported by the current version of Antidote
func (tx *InteractiveTransaction) Abort() error {
	if !tx.committed {
		msg := &ApbAbortTransaction{TransactionDescriptor: tx.txID}
		err := msg.encode(tx.con)
		if err != nil {
			return err
		}
		op, err := decodeOperationResp(tx.con)
		tx.con.Close()
		if err != nil {
			return err
		}
		if !(*op.Success) {
			return fmt.Errorf("operation not successful; error code %d", *op.Errorcode)
		}
	}
	return nil
}

// Pseudo transaction to issue reads and updated without starting an interactive transaction.
// Can be interpreted as starting a transaction for each read or update and directly committing it.
type StaticTransaction struct {
	client *Client
}

func (tx *StaticTransaction) Update(updates ...*ApbUpdateOp) error {
	apbStaticUpdate := &ApbStaticUpdateObjects{
		Transaction: &ApbStartTransaction{Properties: &ApbTxnProperties{}},
		Updates:     updates,
	}
	con, err := tx.client.getConnection()
	if err != nil {
		return err
	}
	err = apbStaticUpdate.encode(con)
	if err != nil {
		return err
	}
	resp, err := decodeCommitResp(con)
	con.Close()
	if err != nil {
		return err
	}
	if !(*resp.Success) {
		return fmt.Errorf("operation not successful; error code %d", *resp.Errorcode)
	}
	return nil
}

func (tx *StaticTransaction) Read(objects ...*ApbBoundObject) (resp *ApbReadObjectsResp, err error) {
	apbRead := &ApbStaticReadObjects{
		Transaction: &ApbStartTransaction{Properties: &ApbTxnProperties{}},
		Objects:     objects,
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
	crdtType := CRDTType_RRMAP
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

// Represents the result of reading from a map object.
// Grants access to the keys of the map to access values of the nested CRDTs.
type MapReadResult struct {
	mapResp *ApbGetMapResp
}

// Access the value of the nested add-wins set under the given key
func (mrr *MapReadResult) Set(key Key) (val [][]byte, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_ORSET && bytes.Equal(me.Key.Key, key) {
			return me.Value.Set.Value, nil
		}
	}
	return nil, fmt.Errorf("set entry with key '%s' not found", key)
}

// Access the value of the nested last-writer-wins register under the given key
func (mrr *MapReadResult) Reg(key Key) (val []byte, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_LWWREG && bytes.Equal(me.Key.Key, key) {
			return me.Value.Reg.Value, nil
		}
	}
	return nil, fmt.Errorf("register entry with key '%s' not found", key)
}

// Access the value of the nested add-wins map under the given key
func (mrr *MapReadResult) Map(key Key) (val *MapReadResult, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_RRMAP && bytes.Equal(me.Key.Key, key) {
			return &MapReadResult{mapResp: me.Value.Map}, nil
		}
	}
	return nil, fmt.Errorf("map entry with key '%s' not found", key)
}

// Access the value of the nested multi-value register under the given key
func (mrr *MapReadResult) MVReg(key Key) (val [][]byte, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_MVREG && bytes.Equal(me.Key.Key, key) {
			return me.Value.Mvreg.Values, nil
		}
	}
	return nil, fmt.Errorf("map entry with key '%s' not found", key)
}

// Access the value of the nested counter under the given key
func (mrr *MapReadResult) Counter(key Key) (val int32, err error) {
	for _, me := range mrr.mapResp.Entries {
		if *me.Key.Type == CRDTType_COUNTER && bytes.Equal(me.Key.Key, key) {
			return *me.Value.Counter.Value, nil
		}
	}
	return 0, fmt.Errorf("counter entry with key '%s' not found", key)
}

// MapEntryKey represents the key and type of a map entry (embedded CRDT).
type MapEntryKey struct {
	Key      []byte
	CrdtType CRDTType
}

// ListMapKeys gives access to the keys and types of map entries (embedded CRDTs).
func (mrr *MapReadResult) ListMapKeys() []MapEntryKey {
	keyList := make([]MapEntryKey, len(mrr.mapResp.Entries))
	for i, me := range mrr.mapResp.Entries {
		keyList[i] = MapEntryKey{
			Key:      me.Key.Key,
			CrdtType: *me.Key.Type,
		}
	}
	return keyList
}

// Represents updates that can be converted to top-level updates applicable to a bucket
// or nested updates applicable to a map
type UpdateConverter interface {
	ConvertToToplevel(bucket []byte) *ApbUpdateOp
	ConvertToNested() *ApbMapNestedUpdate
}

// Represents updates of a specific key of a specific type.
// Can be applied to either buckets or maps
type CRDTUpdate struct {
	Update *ApbUpdateOperation
	Key    Key
	Type   CRDTType
}

// A CRDTUpdater allows to apply updates in the context of a transaction.
type CRDTUpdater interface {
	Update(tx Transaction, updates ...*CRDTUpdate) error
}

func (bucket *Bucket) Update(tx Transaction, updates ...*CRDTUpdate) error {
	updateOps := make([]*ApbUpdateOp, len(updates))
	for i, v := range updates {
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

// Represents the update to add an element to an add-wins set
func SetAdd(key Key, elems ...[]byte) *CRDTUpdate {
	optype := ApbSetUpdate_ADD
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_ORSET,
		Update: &ApbUpdateOperation{
			Setop: &ApbSetUpdate{Adds: elems, Optype: &optype},
		},
	}
}

// Represents the update to remove an element from an add-wins set
func SetRemove(key Key, elems ...[]byte) *CRDTUpdate {
	optype := ApbSetUpdate_REMOVE
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_ORSET,
		Update: &ApbUpdateOperation{
			Setop: &ApbSetUpdate{Rems: elems, Optype: &optype},
		},
	}
}

// Represents the update to increment a counter
func CounterInc(key Key, inc int64) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_COUNTER,
		Update: &ApbUpdateOperation{
			Counterop: &ApbCounterUpdate{Inc: &inc},
		},
	}
}

// Represents the update to write a value into an last-writer-wins register
func RegPut(key Key, value []byte) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_LWWREG,
		Update: &ApbUpdateOperation{
			Regop: &ApbRegUpdate{Value: value},
		},
	}
}

// Represents the update to write a value into an multi-value register
func MVRegPut(key Key, value []byte) *CRDTUpdate {
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_MVREG,
		Update: &ApbUpdateOperation{
			Regop: &ApbRegUpdate{Value: value},
		},
	}
}

// Represents the update to nested objects of an add-wins map
func MapUpdate(key Key, updates ...*CRDTUpdate) *CRDTUpdate {
	nupdates := make([]*ApbMapNestedUpdate, len(updates))
	for i, v := range updates {
		nupdates[i] = v.ConvertToNested()
	}
	return &CRDTUpdate{
		Key:  key,
		Type: CRDTType_RRMAP,
		Update: &ApbUpdateOperation{
			Mapop: &ApbMapUpdate{Updates: nupdates},
		},
	}
}
