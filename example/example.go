package main

import (
	"log"
	"time"

	"github.com/paulmach/ddblock"
	"golang.org/x/net/context"
)

func main() {
	m := ddblock.New(context.Background(), "foo")

	err := m.Lock()
	if ddblock.IsAquireError(err) {
		log.Fatalf("someone already has the lock")
	} else if err != nil {
		// some sort of network error
		panic(err)
	}

	log.Printf("lock acquired, waiting 5 seconds")
	time.Sleep(5 * time.Second)

	// should be able to unlock multiple times

	log.Printf("unlock one")
	err = m.Unlock()
	if err != nil {
		panic(err)
	}

	log.Printf("unlock two")
	err = m.Unlock()
	if err != nil {
		panic(err)
	}

	log.Printf("unlock three")
	err = m.Unlock()
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second)
}
