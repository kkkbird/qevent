package qevent

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/kkkbird/qstream"
)

var (
	defaultWorkCount       = 5
	waitFreeWorkerInterval = 10 * time.Millisecond
	readEventBlockInterval = 10 * time.Millisecond
)

type Handler struct {
	client       *redis.Client
	codec        qstream.DataCodec
	groupName    string
	consumerName string
	workerCount  int
	dataChan     chan eventMsg
}

type eventMsg struct {
	Event   string
	EventID string
	Data    interface{}
}

type DataHandler func(event string, eventId string, data interface{}) error

type HandlerOpts func(e *Handler)

func WithWorkCount(count int) HandlerOpts {
	return func(h *Handler) {
		h.workerCount = count
	}
}

func NewHandler(client *redis.Client, codec qstream.DataCodec, group string, consumer string, opts ...HandlerOpts) *Handler {
	h := &Handler{
		client:       client,
		codec:        codec,
		groupName:    group,
		consumerName: consumer,
		workerCount:  defaultWorkCount,
	}

	for _, o := range opts {
		o(h)
	}

	h.dataChan = make(chan eventMsg, h.workerCount) // workerCount may be changed in opts

	return h
}

func (h *Handler) Run(ctx context.Context, dataHandler DataHandler, events ...string) error {
	// run reader
	readerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go h.reader(readerCtx, events...)

	// run handler
	workerCtx := context.Background() //worker do not use context, but where reader close data chan
	var wg sync.WaitGroup

	wg.Add(h.workerCount)
	for i := 0; i < h.workerCount; i++ {
		go func() {
			defer wg.Done()
			h.handle(workerCtx, dataHandler)
		}()
	}

	select {
	case <-ctx.Done():
		wg.Wait()
	}

	return nil
}

func (h *Handler) reader(ctx context.Context, events ...string) error {
	sub := qstream.NewRedisStreamGroupSub(h.client, h.codec, h.groupName, h.consumerName, false, events...)

	devDataArray := make([]eventMsg, 0, h.workerCount*2)
	var first eventMsg
	var tranChan chan eventMsg

	for {
		if len(devDataArray) > 0 {
			first = devDataArray[0]
			tranChan = h.dataChan
		} else {
			tranChan = nil
		}

		select {
		case <-ctx.Done():
			for _, d := range devDataArray { // TODO: complete all remain data, may add time.After here
				h.dataChan <- d
			}
			close(h.dataChan)
			return nil
		case tranChan <- first:
			devDataArray = devDataArray[1:]
		default:
			if len(devDataArray) > 0 {
				time.Sleep(waitFreeWorkerInterval)
				continue
			}
			result, err := sub.Read(int64(h.workerCount), readEventBlockInterval)
			if err == redis.Nil {
				continue
			}
			if err != nil {
				continue
			}
			if len(result) > 0 {
				for k, v := range result {
					for _, d := range v {
						devDataArray = append(devDataArray, eventMsg{
							Event:   k,
							EventID: d.StreamID,
							Data:    d.Data,
						})
					}
				}
			}
		}
	}
	return nil
}

func (h *Handler) handle(ctx context.Context, dataHandler DataHandler) error {
	var dd eventMsg
	var ok bool
	for {
		select {
		case <-ctx.Done():
			return nil
		case dd, ok = <-h.dataChan:
			if !ok {
				return nil //stop data handler by chan closed
			}
			dataHandler(dd.Event, dd.EventID, dd.Data)
		}
	}

	return nil
}
