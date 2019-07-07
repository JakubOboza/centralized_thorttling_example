package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type Smoothy struct {
	redis             *redis.Client
	maxRps            int
	executionCallback func() error
}

func (sm *Smoothy) throttleKey() string {
	now := time.Now()
	key := fmt.Sprintf("tr:%02d-%02d %d:%02d:%02d", int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second())
	return key
}

func (sm *Smoothy) tryExecute() bool {

	key := sm.throttleKey()

	rawCounter, _ := sm.redis.Get(key).Result()
	counter, _ := strconv.Atoi(rawCounter)

	if rawCounter == "" || counter < sm.maxRps {
		sm.redis.Incr(key)
		sm.redis.Expire(key, time.Second*30)
		sm.executionCallback() // execute throttled callback
		return true
	} else {
		return false
	}

}

func (sm *Smoothy) Run() {
	for {
		if !sm.tryExecute() {
			time.Sleep(15 * time.Millisecond)
			// Wait and retry
		}
	}
}

func main() {

	MAX_VAL := 1000
	bigQueueToFlush := make(chan int, MAX_VAL)
	done := make(chan bool, 1)

	for i := 0; i < MAX_VAL; i++ {
		bigQueueToFlush <- int(i)
	}

	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost"
	}
	redisConnectionString := fmt.Sprintf("%s:6379", redisHost)

	redis_client := redis.NewClient(&redis.Options{
		Addr:     redisConnectionString,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	sm := &Smoothy{
		redis:  redis_client,
		maxRps: 100,
		executionCallback: func() error {
			n := <-bigQueueToFlush
			fmt.Printf("SM: %v\n", n)
			time.Sleep(15 * time.Millisecond)
			if n == MAX_VAL-1 {
				done <- true
			}
			return nil
		},
	}

	sm2 := &Smoothy{
		redis:  redis_client,
		maxRps: 100,
		executionCallback: func() error {
			n := <-bigQueueToFlush
			fmt.Printf("SM2: %v\n", n)
			time.Sleep(15 * time.Millisecond)
			if n == MAX_VAL-1 {
				done <- true
			}
			return nil
		},
	}

	sm3 := &Smoothy{
		redis:  redis_client,
		maxRps: 100,
		executionCallback: func() error {
			n := <-bigQueueToFlush
			fmt.Printf("SM3: %v\n", n)
			time.Sleep(15 * time.Millisecond)
			if n == MAX_VAL-1 {
				done <- true
			}
			return nil
		},
	}

	sm4 := &Smoothy{
		redis:  redis_client,
		maxRps: 100,
		executionCallback: func() error {
			n := <-bigQueueToFlush
			fmt.Printf("SM4: %v\n", n)
			time.Sleep(15 * time.Millisecond)
			if n == MAX_VAL-1 {
				done <- true
			}
			return nil
		},
	}

	go sm2.Run()
	go sm3.Run()
	go sm4.Run()
	go sm.Run()

	<-done

}
