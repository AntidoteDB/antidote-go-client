package antidoteclient

//import "fmt"
import (
	"testing"
	"fmt"
)

func TestSimple(t *testing.T) {

	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	crdtType := CRDTType_COUNTER
	key := &ApbBoundObject{
		Bucket: []byte("bucket"),
		Key:    []byte("key"),
		Type:   &crdtType}
	one := int64(1)
	tx.update(&ApbUpdateOp{
		Boundobject: key,
		Operation:   &ApbUpdateOperation{Counterop: &ApbCounterUpdate{Inc: &one}},
	})
	resp, err := tx.read(key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(resp.Objects[0])

	_, err = tx.commit()
	if err != nil {
		t.Fatal(err)
	}

}
