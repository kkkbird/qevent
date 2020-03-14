package qevent

import (
	"sync"

	"github.com/go-redis/redis/v7"
	"github.com/kkkbird/qstream"
)

var (
	defaultMaxLength int64 = 1000
)

type Emitter struct {
	client       *redis.Client
	defaultCodec qstream.DataCodec
	codecFunc    func(event string) qstream.DataCodec
	eventPubs    sync.Map
	maxLength    int64
}

type EmitterOpts func(e *Emitter)

func WithMaxLength(len int64) EmitterOpts {
	return func(e *Emitter) {
		e.maxLength = len
	}
}

func WithCodec(codec qstream.DataCodec) EmitterOpts {
	return func(e *Emitter) {
		e.defaultCodec = codec
	}
}

func WithCodecFunc(codecFunc func(event string) qstream.DataCodec) EmitterOpts {
	return func(e *Emitter) {
		e.codecFunc = codecFunc
	}
}

func NewEmitter(client *redis.Client, opts ...EmitterOpts) *Emitter {
	e := &Emitter{
		client:    client,
		maxLength: defaultMaxLength,
	}

	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *Emitter) Emit(event string, data interface{}) (string, error) {
	pub, ok := e.eventPubs.Load(event)

	if !ok {
		var codec qstream.DataCodec
		if e.codecFunc != nil {
			codec = e.codecFunc(event)
		}

		if codec == nil {
			if e.defaultCodec != nil {
				codec = e.defaultCodec
			} else {
				codec = qstream.JsonCodec(data)
			}
		}

		pub, _ = e.eventPubs.LoadOrStore(event, qstream.NewRedisStreamPub(e.client, event, e.maxLength, codec))
	}

	return pub.(qstream.StreamPub).Send(data)
}
