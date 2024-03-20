package mq_test

import (
	"context"
	"errors"
	"fmt"
	mq "github.com/qmstar0/MQ"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type Event struct {
	Data string
	Age  int
}

func Test1(t *testing.T) {
	queue := mq.NewQueue[Event]()
	subscribe, err := queue.Subscribe(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup

	go func() {
		for data := range subscribe {
			data.Ack()
			wg.Done()
			t.Log("ok")
		}
	}()

	for i := range 5 {
		wg.Add(1)
		err = queue.Publish(Event{
			Data: "test",
			Age:  i,
		})
		if err != nil {
			t.Error(err)
		}
		time.Sleep(time.Millisecond * 100)
	}
	wg.Wait()
}

// 测试已发布的消息是否能保证消费
func Test2(t *testing.T) {
	queue := mq.NewQueue[Event]()
	subscribe, err := queue.Subscribe(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	time.AfterFunc(time.Second*1, func() {
		defer wg.Done()
		t.Log("start close")
		err := queue.Close()
		if err != nil {
			t.Error("err on close", err)
		}
	})

	go func() {
		for data := range subscribe {
			data := data
			go func() {
				data.Ack()
				wg.Done()
				t.Log("ok")
			}()
			time.Sleep(time.Millisecond * 300)
		}
	}()

	for i := range 5 {
		wg.Add(1)
		err = queue.Publish(Event{
			Data: "test",
			Age:  i,
		})
		if err != nil {
			t.Error(err)
		}
	}
	wg.Wait()
}

func Test3(t *testing.T) {
	background := context.Background()
	queue := mq.NewQueue[Event]()

	ctx1, cc1 := context.WithTimeout(background, time.Second*1)
	defer cc1()
	ctx2, cc2 := context.WithTimeout(background, time.Second*2)
	defer cc2()

	subscribe1, err := queue.Subscribe(ctx1, 2)
	if err != nil {
		t.Fatal(err)
	}
	subscribe2, err := queue.Subscribe(ctx2, 2)
	if err != nil {
		t.Fatal(err)
	}
	var count1 uint32 = 0
	go func() {
		for data := range subscribe1 {
			data.Ack()
			atomic.AddUint32(&count1, 1)
			t.Log("1 ack")
		}
	}()

	var count2 uint32 = 0
	go func() {
		for data := range subscribe2 {
			data.Ack()
			atomic.AddUint32(&count2, 1)
			t.Log("2 ack")
		}
	}()

	for {
		select {
		case <-ctx1.Done():
			err = queue.Publish(Event{
				Data: "test",
				Age:  1,
			})
			if err != nil {
				t.Error(err)
			}
			time.Sleep(time.Millisecond * 100)
		case <-ctx2.Done():
			fmt.Printf("subscribe - 1 processed %d\n", count1)
			fmt.Printf("subscribe - 2 processed %d\n", count2)
			if count1 >= count2 {
				t.Fatal(errors.New("subscribe1处理的数量比subscribe2多，这不符合预期"))
			}
			return
		default:
			err = queue.Publish(Event{
				Data: "test",
				Age:  1,
			})
			if err != nil {
				t.Error(err)
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}
