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
	defer client.Close()

	tx, err := client.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	bucket := Bucket{[]byte("bucket")}
	key := Key("keyCounter")
	err = bucket.Update(tx, CounterInc(key, 1))
	if err != nil {
		t.Fatal(err)
	}

	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(counterVal)

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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

	bucket := Bucket{[]byte("bucket")}
	key := Key("keySet")

	err = bucket.Update(tx, SetAdd(key, []byte("test1")))
	if err != nil {
		t.Fatal(err)
	}


	setVal, err := bucket.ReadSet(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	for _,v := range setVal {
		fmt.Println(string(v))
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

}

func TestManyUpdates(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bucket := Bucket{[]byte("bucket")}
	key := Key("keyMany")

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

func TestReadMany(t *testing.T) {
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

func TestStatic(t *testing.T) {
	client, err := NewClient(Host{"127.0.0.1", 8087})
	if err != nil {
		t.Fatal(err)
	}
	bucket := Bucket{[]byte("bucket")}
	key := Key("keyStatic")
	tx := client.CreateStaticTransaction()


	err = bucket.Update(tx, CounterInc(key, 1))
	if err != nil {
		t.Fatal(err)
	}
	counterVal, err := bucket.ReadCounter(tx, key)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(counterVal)
}
