package qevent

import (
	"context"
	"testing"
	"time"

	"github.com/kkkbird/qstream"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type HandlerTestSuite struct {
	suite.Suite
	redisClient *redis.Client
}

func (s *HandlerTestSuite) SetupSuite() {
	viper.SetDefault("redis.url", "192.168.1.233:30790")
	viper.SetDefault("redis.password", "12345678")

	s.redisClient = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.url"),
		Password: viper.GetString("redis.password"),
		DB:       0,
	})
}

func (s *HandlerTestSuite) TearDownSuite() {
	s.redisClient.Close()
}

func (s *HandlerTestSuite) TearDownTest() {
	s.redisClient.Del("qevent:test")
}

func (s *HandlerTestSuite) SetupTest() {
	s.redisClient.Del("qevent:test")
}

func (s *HandlerTestSuite) emitSimpleMsgs(event string, count int) (map[int]string, error) {
	emitter := NewEmitter(s.redisClient)
	msgIDs := make(map[int]string)

	for i := 0; i < count; i++ {
		d := &SimpleData{
			ID:      i,
			Message: "emit event",
		}

		streamID, err := emitter.Emit(event, d)
		if err != nil {
			return nil, err
		}
		msgIDs[i] = streamID
	}
	return msgIDs, nil
}

func (s *HandlerTestSuite) getPendingAndReading(event string, group string, consumer string) (int, int, error) {
	rlt, err := s.redisClient.XReadGroup(&redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{event, "0-0"},
		Count:    20,
		Block:    time.Millisecond,
		NoAck:    false,
	}).Result()

	if err != nil {
		return 0, 0, err
	}

	log.Info("getPending:", rlt)

	pending := len(rlt[0].Messages)

	rlt, err = s.redisClient.XReadGroup(&redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{event, ">"},
		Count:    20,
		Block:    time.Millisecond,
		NoAck:    false,
	}).Result()

	if err == redis.Nil {
		return pending, 0, nil
	}
	if err != nil {
		return 0, 0, nil
	}

	reading := len(rlt[0].Messages)
	return pending, reading, nil
}

func (s *HandlerTestSuite) runHandler(ctx context.Context, event, group, consume string, duration time.Duration, opts ...HandlerOpts) (chan int, chan error) {

	handler := NewHandler(s.redisClient, qstream.JsonCodec(SimpleData{}), group, consume, opts...)

	handleChan := make(chan int, 1)
	exitChan := make(chan error, 1)

	go func() {
		exitChan <- handler.Run(ctx, func(event string, eventId string, data interface{}) error {
			d := data.(*SimpleData)
			log.Infof("%s start %d, event=%s,id=%s,data=%#v", consume, d.ID, event, eventId, data)
			time.Sleep(duration)
			log.Infof("%s done %d, event=%s,id=%s done", consume, d.ID, event, eventId)
			handleChan <- 1
			return nil
		}, event)
	}()

	return handleChan, exitChan

}

func (s *HandlerTestSuite) TestHandleSimple() {
	event := "qevent:test"
	expectedCount := 13
	msgIds, err := s.emitSimpleMsgs(event, expectedCount)

	if !s.NoError(err) {
		return
	}
	log.Infof("emit [%d]msg: %#v", len(msgIds), msgIds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleChan, exitChan := s.runHandler(ctx, event, "testgroup", "handle1", time.Second, WithWorkCount(5))

	testTimeout := time.After(time.Second * 20)
	var handledCount int

	for {
		select {
		case i := <-handleChan:
			handledCount += i
			log.Infof("handled:%d, expected:%d", handledCount, expectedCount)
			if handledCount == expectedCount {
				cancel()
			}
		case <-exitChan:
			log.Info("Get exit!!!!!!!!!!!!")
			if !s.Equal(handledCount, expectedCount, "handled event should equal to expected") {
				return
			}
			pending, reading, err := s.getPendingAndReading(event, "testgroup", "handle1")
			log.Infof("pending:%d, reading=%d", pending, reading)

			if !s.NoError(err, "get pending item error") {
				return
			}

			if !s.Zero(pending, "pending event should be 0") {
				return
			}

			if !s.Zero(reading, "pending event should be 0") {
				return
			}

			return
		case <-testTimeout:
			s.FailNow("should not timeout here")
			return
		}
	}
}

func (s *HandlerTestSuite) TestHandleSimpleClose() {
	event := "qevent:test"
	expectedCount := 23
	msgIds, err := s.emitSimpleMsgs(event, expectedCount)

	if !s.NoError(err) {
		return
	}
	log.Infof("emit [%d]msg: %#v", len(msgIds), msgIds)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	handleChan, exitChan := s.runHandler(ctx, event, "testgroup", "handle1", time.Second, WithWorkCount(2))

	testTimeout := time.After(time.Second * 20)
	var handledCount int

	for {
		select {
		case i := <-handleChan:
			handledCount += i
			log.Infof("handled:%d, expected:%d", handledCount, expectedCount)
			if handledCount == expectedCount {
				cancel()
			}
		case <-exitChan:
			log.Info("Get exit!!!!!!!!!!!!")
			if !s.NotEqual(handledCount, expectedCount, "handled event should not equal to expected") {
				return
			}
			pending, reading, err := s.getPendingAndReading(event, "testgroup", "handle1")
			log.Infof("pending:%d, reading=%d", pending, reading)

			if !s.NoError(err, "get pending item error") {
				return
			}

			if !s.Zero(pending, "pending event should be 0") {
				return
			}

			if !s.NotZero(reading, "pending event should not be 0") {
				return
			}

			return
		case <-testTimeout:
			s.FailNow("should not timeout here")
			return
		}
	}
}

func (s *HandlerTestSuite) TestHandleSimpleWithCloseTimeout() {
	event := "qevent:test"
	expectedCount := 23
	msgIds, err := s.emitSimpleMsgs(event, expectedCount)

	if !s.NoError(err) {
		return
	}
	log.Infof("emit [%d]msg: %#v", len(msgIds), msgIds)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	handleChan, exitChan := s.runHandler(ctx, event, "testgroup", "handle1", time.Second, WithWorkCount(2), WithCloseTimeout(time.Millisecond*50))

	testTimeout := time.After(time.Second * 20)
	var handledCount int

	for {
		select {
		case i := <-handleChan:
			handledCount += i
			log.Infof("handled:%d, expected:%d", handledCount, expectedCount)
			if handledCount == expectedCount {
				cancel()
			}
		case <-exitChan:
			log.Info("Get exit!!!!!!!!!!!!")
			if !s.NotEqual(handledCount, expectedCount, "handled event should not equal to expected") {
				return
			}
			pending, reading, err := s.getPendingAndReading(event, "testgroup", "handle1")
			log.Infof("pending:%d, reading=%d", pending, reading)

			if !s.NoError(err, "get pending item error") {
				return
			}

			if !s.NotZero(pending, "pending event should not be 0") {
				return
			}

			if !s.NotZero(reading, "pending event should not be 0") {
				return
			}

			return
		case <-testTimeout:
			s.FailNow("should not timeout here")
			return
		}
	}
}

func (s *HandlerTestSuite) TestHandleSimpleWithoutCloseTimeout() {
	event := "qevent:test"
	emitter := NewEmitter(s.redisClient)

	d := &SimpleData{
		ID:      2,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(event, d)

	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)

	handler := NewHandler(s.redisClient, qstream.JsonCodec(SimpleData{}), "testgroup", "testconsumer")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	handler.Run(ctx, func(event string, eventId string, data interface{}) error {
		log.Infof("recv event=%s,id=%s,data=%#v", event, eventId, data)
		time.Sleep(time.Second * 10)
		log.Infof("recv event=%s,id=%s done", event, eventId)
		return nil
	}, event)
}

func (s *HandlerTestSuite) TestHandleSimpleWithCheckPending() {
	event := "qevent:test"
	expectedCount := 13
	msgIds, err := s.emitSimpleMsgs(event, expectedCount)

	if !s.NoError(err) {
		return
	}
	log.Infof("emit [%d]msg: %#v", len(msgIds), msgIds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handleChanPending, _ := s.runHandler(ctx, event, "testgroup", "handle1", time.Second*100, WithWorkCount(4))

	time.Sleep(time.Second * 2)
	handleChan, exitChan := s.runHandler(ctx, event, "testgroup", "handle2", time.Second, WithCheckPending(5*time.Second))

	testTimeout := time.After(time.Second * 20)
	var handledCount int

	for {
		select {
		case <-handleChanPending:
			cancel()
			s.FailNow("pending chan should not be handled")
			return
		case i := <-handleChan:
			handledCount += i
			log.Infof("handled:%d, expected:%d", handledCount, expectedCount)
			if handledCount == expectedCount {
				cancel()
			}
		case <-exitChan:
			log.Info("Get exit!!!!!!!!!!!!")
			if !s.Equal(handledCount, expectedCount, "handled event should equal to expected") {
				return
			}
			pending, reading, err := s.getPendingAndReading(event, "testgroup", "handle1")
			log.Infof("pending:%d, reading=%d", pending, reading)

			if !s.NoError(err, "get pending item error") {
				return
			}

			if !s.Zero(pending, "pending event should be 0") {
				return
			}

			if !s.Zero(reading, "pending event should be 0") {
				return
			}

			return
		case <-testTimeout:
			cancel()
			s.FailNow("should not timeout here")
			return
		}
	}
}

func TestHandler(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}
