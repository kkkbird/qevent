package qevent

import (
	"context"
	"testing"
	"time"

	"github.com/kkkbird/qstream"
	"github.com/redis/go-redis/v9"

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
		Addr:                  viper.GetString("redis.url"),
		Password:              viper.GetString("redis.password"),
		DB:                    0,
		ContextTimeoutEnabled: true,
	})
}

func (s *HandlerTestSuite) TearDownSuite() {
	s.redisClient.Close()
}

func (s *HandlerTestSuite) TearDownTest() {
	s.redisClient.Del(context.Background(), "qevent:test")
}

func (s *HandlerTestSuite) SetupTest() {
	s.redisClient.Del(context.Background(), "qevent:test")
}

func (s *HandlerTestSuite) emitSimpleMsgs(event string, count int) (map[int]string, error) {
	emitter := NewEmitter(s.redisClient)
	msgIDs := make(map[int]string)
	ctx := context.Background()

	for i := 0; i < count; i++ {
		d := &SimpleData{
			ID:      i,
			Message: "emit event",
		}

		streamID, err := emitter.Emit(ctx, event, d)
		if err != nil {
			return nil, err
		}
		msgIDs[i] = streamID
	}
	return msgIDs, nil
}

func (s *HandlerTestSuite) getPendingAndReading(event string, group string, consumer string) (int, int, error) {
	ctx := context.Background()
	rlt, err := s.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
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

	rlt, err = s.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
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
		exitChan <- handler.Run(ctx, func(msg EventMsg) error {
			d := msg.Data.(*SimpleData)
			log.Infof("%s start %d, event=%s,id=%s,data=%#v", consume, d.ID, msg.Event, msg.EventID, msg.Data)
			time.Sleep(duration)
			log.Infof("%s done %d, event=%s,id=%s done", consume, d.ID, msg.Event, msg.EventID)
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
	ctx := context.Background()

	d := &SimpleData{
		ID:      2,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(ctx, event, d)

	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)

	handler := NewHandler(s.redisClient, qstream.JsonCodec(SimpleData{}), "testgroup", "testconsumer")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	handler.Run(ctx, func(msg EventMsg) error {
		log.Infof("recv event=%s,id=%s,data=%#v", msg.Event, msg.EventID, msg.Data)
		time.Sleep(time.Second * 10)
		log.Infof("recv event=%s,id=%s done", msg.Event, msg.EventID)
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

func (s *HandlerTestSuite) TestHandleBugXClaim() {
	client := s.redisClient
	ctx := context.Background()

	var maxLen int64 = 2

	client.Del(ctx, "mystream")
	client.XGroupCreateMkStream(ctx, "mystream", "mygroup", "$")

	// add 2 entries
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		MaxLen: maxLen,
		Values: map[string]interface{}{"value": 1},
	})
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		MaxLen: maxLen,
		Values: map[string]interface{}{"value": 2},
	})

	// read by alice
	client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "mygroup",
		Consumer: "alice",
		Streams:  []string{"mystream", ">"},
		Count:    10,
		Block:    0,
		NoAck:    false,
	})

	// add 2 more entries
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		MaxLen: maxLen,
		Values: map[string]interface{}{"value": 3},
	})
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		MaxLen: maxLen,
		Values: map[string]interface{}{"value": 4},
	})

	// read by alice again, now alice has 4 pending messages, and 2 messages were trimed
	client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "mygroup",
		Consumer: "alice",
		Streams:  []string{"mystream", ">"},
		Count:    10,
		Block:    0,
		NoAck:    false,
	}).Result()

	// sleep for a while
	time.Sleep(3 * time.Second)

	// get all pending messages
	pending, _ := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "mystream",
		Group:  "mygroup",
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()

	claimIds := make([]string, 0)
	for _, p := range pending {
		claimIds = append(claimIds, p.ID)
	}

	// bob claim messages, on redis-cli, the result will be:
	// 1) (nil)
	// 2) (nil)
	// 3) 1) "1575368938045-0"
	//    2) 1) "value"
	// 	  2) "3"
	// 4) 1) "1575371014539-0"
	//    2) 1) "value"
	// 	  2) "4"
	rlt, err := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   "mystream",
		Group:    "mygroup",
		Consumer: "bob",
		MinIdle:  time.Second,
		Messages: claimIds,
	}).Result()

	// rlt will be empty array with error redis.Nil
	log.Info(rlt, " ", err)

	for i := 0; i < 4; i++ {
		len, err := client.XLen(ctx, "mystream").Result() // redis call will fail serval times
		log.Info(i, " ", len, " ", err)
	}
}

func TestHandler(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}
