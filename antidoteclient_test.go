package antidoteclient

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSimple(t *testing.T) {

	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().Unix()
	bucketname := fmt.Sprintf("bucket%d", timestamp)
	bucket := Bucket{[]byte(bucketname)}
	key := Key("keyCounter")
	err = bucket.Update(tx, CounterInc(key, 1))
	if err != nil {
		t.Fatal(err)
	}

	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	if counterVal != 1 {
		t.Fatalf("Counter value should be 1 but is %d", counterVal)
	}
}

func TestSetUpdate(t *testing.T) {

	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().Unix()
	bucketname := fmt.Sprintf("bucket%d", timestamp)
	bucket := Bucket{[]byte(bucketname)}
	key := Key("keySet")

	err = bucket.Update(tx, SetAdd(key, []byte("test1"), []byte("value2"), []byte("inset3")))
	if err != nil {
		t.Fatal(err)
	}

	setVal, err := bucket.ReadSet(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for _, expected := range []string{"test1", "value2", "inset3"} {
		found := false
		for _, val := range setVal {
			if string(val) == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected value %s not found in result (%s)", expected, setVal)
		}
	}
}

func TestMap(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().Unix()
	bucketname := fmt.Sprintf("bucket%d", timestamp)
	bucket := Bucket{[]byte(bucketname)}
	key := Key("keyMap")

	err = bucket.Update(tx, MapUpdate(key,
		CounterInc(Key("counter"), 13),
		RegPut(Key("reg"), []byte("Hello World")),
		SetAdd(Key("set"), []byte("A"), []byte("B"))))
	if err != nil {
		t.Fatal(err)
	}

	mapVal, err := bucket.ReadMap(tx, key)
	if err != nil {
		t.Fatal(err)
	}
	if v, e := mapVal.Counter(Key("counter")); e != nil || v != 13 {
		t.Fatalf("Wrong counter value: %d", v)
	}
	if v, e := mapVal.Reg(Key("reg")); e != nil || !bytes.Equal(v, []byte("Hello World")) {
		t.Fatalf("Wrong reg value: %p", v)
	}
	v, _ := mapVal.Set(Key("set"))
	if len(v) != 2 {
		t.Fatal("Wrong number of elements in set")
	}
	for _, expected := range []string{"A", "B"} {
		found := false
		for _, val := range v {
			if string(val) == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected value %s not found in result (%s)", expected, v)
		}
	}
}

func TestStatic(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	timestamp := time.Now().Unix()
	bucketname := fmt.Sprintf("bucket%d", timestamp)
	bucket := Bucket{[]byte(bucketname)}
	key := Key("keyStatic")
	tx := client.CreateStaticTransaction()

	err = bucket.Update(tx, CounterInc(key, 42))
	if err != nil {
		t.Fatal(err)
	}
	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	if counterVal != 42 {
		t.Fatalf("Counter value should be 42 but is %d", counterVal)
	}
}

// tests for many updates, not enabled

// this is a bit faster than the sequential one, if number of threads in configured correctly
func testManyUpdates(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bucket := Bucket{[]byte("bucket")}
	key := Key("keyMany")

	wg := sync.WaitGroup{}

	numThreads := 5

	wg.Add(numThreads)
	for k := 0; k < numThreads; k++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 6000; i++ {
				tx, err := client.StartTransaction()
				if err != nil {
					t.Fatal(err)
				}
				err = bucket.Update(tx, CounterInc(key, 1))
				if err != nil {
					t.Fatal(err)
				}
				err = tx.Commit()
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

	tx := client.CreateStaticTransaction()
	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(counterVal)
}

// not as fast as the parallel version, but close
func testManyUpdatesSeq(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bucket := Bucket{[]byte("bucket")}
	key := Key("keyManySeq")

	for i := 0; i < 30000; i++ {
		tx, err := client.StartTransaction()
		if err != nil {
			t.Fatal(err)
		}
		err = bucket.Update(tx, CounterInc(key, 1))
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		if i%1000 == 0 {
			fmt.Println(i)
		}
	}

	tx := client.CreateStaticTransaction()
	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(counterVal)
}

// do not issue too many operations in one transaction, it will blow up the transaction cache!
func testManyUpdatesSeqInTrans(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bucket := Bucket{[]byte("bucket")}
	key := Key("keyManySeqTrans")

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30000; i++ {
		err = bucket.Update(tx, CounterInc(key, 1))
		if err != nil {
			t.Fatal(err)
		}

		if i%1000 == 0 {
			fmt.Println(i)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	txs := client.CreateStaticTransaction()
	counterVal, err := bucket.ReadCounter(txs, key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(counterVal)
}

func testReadMany(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bucket := Bucket{[]byte("bucket")}
	key := Key("keyMany")
	tx := client.CreateStaticTransaction()
	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(counterVal)
}
