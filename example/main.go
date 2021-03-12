package main

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/kkkbird/qevent"
	_ "github.com/kkkbird/qlog"
	"github.com/kkkbird/qstream"

	log "github.com/sirupsen/logrus"
)

const (
	TestEventName        = "qevent-test"
	TestEventGroup       = "qevent-testgroup"
	TestEventConsumer    = "qevent-testconsumer"
	TestEventWorkerCount = 5
)

type TestEvent struct {
	Content string
	Data    int
}

func NewTestEvent(content string, data int) *TestEvent {
	return &TestEvent{
		Content: content,
		Data:    data,
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:         "192.168.1.231:30790",
		Password:     "12345678",
		PoolSize:     10,
		MinIdleConns: 5,
		DB:           0,
	})

	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Error("init redis fail:", err)
		return
	}

	// init event emitter
	emitter := qevent.NewEmitter(rdb, qevent.WithCodecFunc(func(event string) qstream.DataCodec {
		// we can just return qstream.JsonCodec(TestEvent{}) if only one event is used
		switch event {
		case TestEventName:
			return qstream.JsonCodec(TestEvent{})
		}
		log.Warn("unknow event:", event)
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())

	// emit some event in the routine
	go func() {
		for i := 0; i < 3; i++ {
			emitter.Emit(ctx, TestEventName, NewTestEvent("hello world", i))
			time.Sleep(time.Second)
		}
		emitter.Emit(ctx, TestEventName, NewTestEvent("goodbye", -1))
	}()

	// init event handler
	handler := qevent.NewHandler(rdb, qstream.JsonCodec(TestEvent{}), TestEventGroup, TestEventConsumer,
		qevent.WithWorkCount(TestEventWorkerCount))

	// call handler.Run to handle the event
	go handler.Run(ctx, func(msg qevent.EventMsg) error {
		if msg.Err != nil {
			log.Warnf("run test handler err:%v", msg) // when msg was trimmed by redis, we could got ErrMessageTrimmed and may take care of it.
			return msg.Err
		}

		e := msg.Data.(*TestEvent)

		log.Infof("receive event(%d): %s", e.Data, e.Content)

		if e.Data < 0 {
			cancel()
		}

		return nil
	}, TestEventName)

	<-ctx.Done()
	log.Info("Done!!")
}
