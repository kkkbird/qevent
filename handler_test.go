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

func (s *HandlerTestSuite) TestHandleSimple() {
	event := "qevent:test"
	emitter := NewEmitter(s.redisClient)

	d := &SimpleData{
		ID:      456,
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
		time.Sleep(time.Second * 3)
		log.Infof("recv event=%s,id=%s done", event, eventId)
		return nil
	}, event)
}

func (s *HandlerTestSuite) TestHandleSimpleWithoutCloseTimeout() {
	event := "qevent:test"
	emitter := NewEmitter(s.redisClient)

	d := &SimpleData{
		ID:      456,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(event, d)

	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)

	handler := NewHandler(s.redisClient, qstream.JsonCodec(SimpleData{}), "testgroup", "testconsumer", WithCloseTimeout(2 * time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	handler.Run(ctx, func(event string, eventId string, data interface{}) error {
		log.Infof("recv event=%s,id=%s,data=%#v", event, eventId, data)
		time.Sleep(time.Second * 10)
		log.Infof("recv event=%s,id=%s done", event, eventId)
		return nil
	}, event)
}

func (s *HandlerTestSuite) TestHandleSimpleWithCloseTimeout() {
	event := "qevent:test"
	emitter := NewEmitter(s.redisClient)

	d := &SimpleData{
		ID:      456,
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

func TestHandler(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}
