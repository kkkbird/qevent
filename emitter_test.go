package qevent

import (
	"context"
	"testing"

	"github.com/kkkbird/qstream"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type SimpleData struct {
	ID      int
	Message string
}

type SimpleData2 struct {
	ID      int
	Gendor  int
	Enabled bool
}

type EmitterTestSuite struct {
	suite.Suite
	redisClient *redis.Client
}

func (s *EmitterTestSuite) SetupSuite() {
	viper.SetDefault("redis.url", "192.168.1.233:30790")
	viper.SetDefault("redis.password", "12345678")

	s.redisClient = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.url"),
		Password: viper.GetString("redis.password"),
		DB:       0,
	})
}

func (s *EmitterTestSuite) TearDownSuite() {
	ctx := context.Background()
	s.redisClient.Del(ctx, "qevent:test")
	s.redisClient.Del(ctx, "qevent:test2")
	s.redisClient.Close()
}

func (s *EmitterTestSuite) TestEmitSimple() {
	ctx := context.Background()
	event := "qevent:test"
	emitter := NewEmitter(s.redisClient)

	d := &SimpleData{
		ID:      4567,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(ctx, event, d)

	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)
}

func (s *EmitterTestSuite) TestEmitWithCodec() {
	ctx := context.Background()
	event := "qevent:test"
	emitter := NewEmitter(s.redisClient, WithCodec(qstream.JsonCodec(SimpleData{})))

	d := &SimpleData{
		ID:      4567,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(ctx, event, d)

	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)
}

func (s *EmitterTestSuite) TestEmitMultiEvent() {
	ctx := context.Background()

	event1 := "qevent:test"
	event2 := "qevent:test2"
	emitter := NewEmitter(s.redisClient)

	d := &SimpleData{
		ID:      4567,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(ctx, event1, d)
	if !s.NoError(err) {
		return
	}

	d2 := &SimpleData2{
		ID:      4567,
		Gendor:  1,
		Enabled: true,
	}
	streamid, err = emitter.Emit(ctx, event2, d2)
	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)
}

func (s *EmitterTestSuite) TestEmitWithCodecFunc() {
	ctx := context.Background()

	event1 := "qevent:test"
	event2 := "qevent:test2"
	emitter := NewEmitter(s.redisClient, WithCodecFunc(func(event string) qstream.DataCodec {
		switch event {
		case event1:
			return qstream.MsgpackCodec(SimpleData{})
		case event2:
			return qstream.StructCodec(SimpleData2{})
		}
		return nil
	}))

	d := &SimpleData{
		ID:      4567,
		Message: "emit event",
	}

	streamid, err := emitter.Emit(ctx, event1, d)
	if !s.NoError(err) {
		return
	}

	d2 := &SimpleData2{
		ID:      4567,
		Gendor:  1,
		Enabled: true,
	}
	streamid, err = emitter.Emit(ctx, event2, d2)
	if !s.NoError(err) {
		return
	}
	log.Infof(streamid)
}

func TestEmitter(t *testing.T) {
	suite.Run(t, new(EmitterTestSuite))
}
