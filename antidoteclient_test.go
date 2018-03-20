package antidoteclient

//import "fmt"
import (
	"testing"
	"fmt"
	"sync"
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
	crdtType := CRDTType_ORSET
	key := &ApbBoundObject{
		Bucket: []byte("bucket"),
		Key:    []byte("keySet"),
		Type:   &crdtType}
	addType := ApbSetUpdate_ADD
	tx.Update(&ApbUpdateOp{
		Boundobject: key,
		Operation:   &ApbUpdateOperation{Setop: &ApbSetUpdate{Optype: &addType, Adds: [][]byte{[]byte("test1")}}},
	})
	resp, err := tx.Read(key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(resp.Objects[0])

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	client.Close()
}

func TestSetUpdate(t *testing.T) {

	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	crdtType := CRDTType_ORSET
	key := &ApbBoundObject{
		Bucket: []byte("bucket"),
		Key:    []byte("keySet"),
		Type:   &crdtType}
	addType := ApbSetUpdate_ADD
	tx.Update(&ApbUpdateOp{
		Boundobject: key,
		Operation:   &ApbUpdateOperation{Setop: &ApbSetUpdate{Optype: &addType, Adds: [][]byte{[]byte("test1")}}},
	})
	resp, err := tx.Read(key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(resp.Objects[0])

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

}

func TestManyUpdates(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}

	crdtType := CRDTType_COUNTER
	key := &ApbBoundObject{
		Bucket: []byte("bucket"),
		Key:    []byte("keyMany"),
		Type:   &crdtType}
	one := int64(1)

	wg := sync.WaitGroup{}
	wg.Add(10)
	for k:=0; k<10; k++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				tx, err := client.StartTransaction()
				if err != nil {
					t.Fatal(err)
				}
				_, err = tx.Update(&ApbUpdateOp{
					Boundobject: key,
					Operation:   &ApbUpdateOperation{Counterop: &ApbCounterUpdate{Inc: &one}},
				})
				if err != nil {
					t.Fatal(err)
				}
				_, err = tx.Commit()
				if err != nil {
					t.Fatal(err)
				}
				if i%1000 == 0 {
					fmt.Println(i)
				}
			}
		}()
	}
	wg.Wait()

	resp, err := client.StaticRead(key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(resp.Objects.Objects[0])
}

func TestStatic(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}

	crdtType := CRDTType_COUNTER
	key := &ApbBoundObject{
		Bucket: []byte("bucket"),
		Key:    []byte("keyStatic"),
		Type:   &crdtType}
	one := int64(1)
	_, err = client.StaticUpdate(&ApbUpdateOp{
		Boundobject: key,
		Operation:   &ApbUpdateOperation{Counterop: &ApbCounterUpdate{Inc: &one}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.StaticRead(key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(resp.Objects.Objects[0])
}
