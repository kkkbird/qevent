package qevent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/kkkbird/qstream"
)

var (
	defaultWorkCount       = 5
	defaultGroupStartID    = "0-0"
	waitFreeWorkerInterval = 10 * time.Millisecond
	readEventBlockInterval = 10 * time.Millisecond
	ErrCloseTimeout        = errors.New("Close timeout")
)

type Handler struct {
	client               *redis.Client
	codec                qstream.DataCodec
	groupName            string
	groupStartID         string
	consumerName         string
	workerCount          int
	dataChan             chan eventMsg
	ackChan              chan eventAck
	closeTimeoutDuration time.Duration
}

type eventMsg struct {
	Event   string
	EventID string
	Data    interface{}
}

type eventAck struct {
	Event   string
	EventID string
	Err     error
}

type DataHandler func(event string, eventId string, data interface{}) error

type HandlerOpts func(e *Handler)

func WithWorkCount(count int) HandlerOpts {
	return func(h *Handler) {
		h.workerCount = count
	}
}

func WithGroupStartID(startID string) HandlerOpts {
	return func(h *Handler) {
		h.groupStartID = startID
	}
}

func WithCloseTimeout(d time.Duration) HandlerOpts {
	return func(h *Handler) {
		h.closeTimeoutDuration = d
	}
}

func NewHandler(client *redis.Client, codec qstream.DataCodec, group string, consumer string, opts ...HandlerOpts) *Handler {
	h := &Handler{
		client:               client,
		codec:                codec,
		groupName:            group,
		consumerName:         consumer,
		workerCount:          defaultWorkCount,
		groupStartID:         defaultGroupStartID,
		closeTimeoutDuration: 0, // 0 means never timeout
	}

	for _, o := range opts {
		o(h)
	}

	h.dataChan = make(chan eventMsg, h.workerCount) // workerCount may be changed in opts
	h.ackChan = make(chan eventAck, h.workerCount)

	return h
}

func (h *Handler) Run(ctx context.Context, dataHandler DataHandler, events ...string) error {
	// run handler
	go func() {
		var wg sync.WaitGroup
		wg.Add(h.workerCount)

		for i := 0; i < h.workerCount; i++ {
			go func() {
				defer wg.Done()

				for dd := range h.dataChan {
					err := dataHandler(dd.Event, dd.EventID, dd.Data)

					h.ackChan <- eventAck{
						Event:   dd.Event,
						EventID: dd.EventID,
						Err:     err,
					}
				}
			}()
		}

		wg.Wait() // wait all worker exit
		close(h.ackChan)
	}()
	// run reader
	return h.reader(ctx, events...)
}

func (h *Handler) reader(ctx context.Context, events ...string) error {
	var (
		first     eventMsg
		transChan chan eventMsg
		doneChan  <-chan struct{}
	)

	forceCloseCtx := context.Background()
	eventArray := make([]eventMsg, 0, h.workerCount*2)
	doneChan = ctx.Done()

	sub := qstream.NewRedisStreamGroupSub(h.client, h.codec, h.groupName, h.groupStartID, h.consumerName, false, events...)
	lastIDs := make([]string, len(events))
	for i := 0; i < len(events); i++ {
		lastIDs[i] = "0-0"
	}

	for {
		if len(eventArray) > 0 {
			first = eventArray[0]
			transChan = h.dataChan
		} else {
			transChan = nil
		}

		select {
		case <-doneChan:
			doneChan = nil
			if h.closeTimeoutDuration > 0 {
				forceCloseCtx, _ = context.WithTimeout(forceCloseCtx, h.closeTimeoutDuration)
			}
		case <-forceCloseCtx.Done():
			return ErrCloseTimeout
		case transChan <- first:
			eventArray = eventArray[1:]
		case ack := <-h.ackChan:
			sub.Ack(ack.Event, ack.EventID) // TODO: we may check error to handle ack later
		default:
			if len(eventArray) > 0 {
				time.Sleep(waitFreeWorkerInterval) // sleep if no free worker, TBU
				continue
			}

			if doneChan == nil { // no event in list and Done
				close(h.dataChan) // close dataChan to make worker exit

				for {
					select {
					case <-forceCloseCtx.Done():
						return ErrCloseTimeout
					case ack, ok := <-h.ackChan:
						if !ok {
							return nil
						}
						sub.Ack(ack.Event, ack.EventID)
					}
				}

				return nil
			}

			result, err := sub.Read(int64(h.workerCount), readEventBlockInterval, lastIDs...) // won't return redis.Nil if checkPending

			if err == redis.Nil {
				continue
			}

			if err != nil {
				time.Sleep(time.Second) // TODO: error should be handled, now we just sleep 1 second and continue,
				continue
			}

			for k, v := range result {
				idx := sub.GetKeyIndex(k)
				if len(v) == 0 {
					lastIDs[idx] = ">"
				} else {
					for _, d := range v {
						eventArray = append(eventArray, eventMsg{
							Event:   k,
							EventID: d.StreamID,
							Data:    d.Data,
						})
						lastIDs[idx] = d.StreamID
					}
				}
			}
		}
	}
}
