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
	defaultGroupStartID    = "0-0"
	waitFreeWorkerInterval = 10 * time.Millisecond
	readEventBlockInterval = 10 * time.Millisecond
)

type Handler struct {
	client       *redis.Client
	codec        qstream.DataCodec
	groupName    string
	groupStartID string
	consumerName string
	workerCount  int
	noAck        bool
	dataChan     chan eventMsg
	ackChan      chan eventAck
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

func WithAck() HandlerOpts {
	return func(h *Handler) {
		h.noAck = false
	}
}

func NewHandler(client *redis.Client, codec qstream.DataCodec, group string, consumer string, opts ...HandlerOpts) *Handler {
	h := &Handler{
		client:       client,
		codec:        codec,
		groupName:    group,
		consumerName: consumer,
		workerCount:  defaultWorkCount,
		groupStartID: defaultGroupStartID,
		noAck:        true, //no ack by default, use WithAck() to enable ack
	}

	for _, o := range opts {
		o(h)
	}

	h.dataChan = make(chan eventMsg, h.workerCount) // workerCount may be changed in opts

	if !h.noAck {
		h.ackChan = make(chan eventAck, h.workerCount)
	}

	return h
}

func (h *Handler) Run(ctx context.Context, dataHandler DataHandler, events ...string) error {
	// run reader
	readerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	readerExitChan := make(chan error, 1)

	go func() {
		readerExitChan <- h.reader(readerCtx, events...)
	}()

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
		wg.Wait() // wait all worker exit

		if !h.noAck { // close ack chan to make reader exit
			close(h.ackChan)
		}
		<-readerExitChan // wait reader exit
	}

	return nil
}

func (h *Handler) reader(ctx context.Context, events ...string) error {
	var (
		first    eventMsg
		tranChan chan eventMsg
		result   map[string][]qstream.StreamSubResult
		err      error
		lastIDs  []string
	)
	sub := qstream.NewRedisStreamGroupSub(h.client, h.codec, h.groupName, h.groupStartID, h.consumerName, h.noAck, events...)

	checkPending := !h.noAck
	if checkPending {
		lastIDs = make([]string, len(events))
		for i := 0; i < len(events); i++ {
			lastIDs[i] = "0-0"
		}
	}

	eventArray := make([]eventMsg, 0, h.workerCount*2)

	for {
		if len(eventArray) > 0 {
			first = eventArray[0]
			tranChan = h.dataChan
		} else {
			tranChan = nil
		}

		select {
		case <-ctx.Done():
			// TODO: complete all remain data, may add time.After here
			for _, d := range eventArray {
				h.dataChan <- d
			}
			close(h.dataChan) // close dataChan to make worker exit
			if !h.noAck {
				for ack := range h.ackChan {
					sub.Ack(ack.Event, ack.EventID)
				}
			}
			return nil
		case tranChan <- first:
			eventArray = eventArray[1:]
		case ack := <-h.ackChan:
			sub.Ack(ack.Event, ack.EventID) // TODO: we may check error to handle ack later
		default:
			if len(eventArray) > 0 {
				time.Sleep(waitFreeWorkerInterval) // sleep if no free worker, TBU
				continue
			}

			if checkPending {
				result, err = sub.Read(int64(h.workerCount), 0, lastIDs...) // won't return redis.Nil if checkPending
			} else {
				result, err = sub.Read(int64(h.workerCount), readEventBlockInterval)
				if err == redis.Nil {
					continue
				}
			}

			if err != nil {
				time.Sleep(time.Second) // TODO: error should be handled, now we just sleep 1 second and continue,
				continue
			}

			if checkPending { // if all value has read and update lastID
				hasData := false
				for k, v := range result {
					if len(v) > 0 {
						hasData = true
						lastIDs[sub.GetKeyIndex(k)] = v[len(v)-1].StreamID
					}
				}

				if !hasData {
					checkPending = false
				}
			}

			for k, v := range result {
				for _, d := range v {
					eventArray = append(eventArray, eventMsg{
						Event:   k,
						EventID: d.StreamID,
						Data:    d.Data,
					})
				}
			}
		}
	}
	return nil
}

func (h *Handler) handle(ctx context.Context, dataHandler DataHandler) error {
	var (
		dd  eventMsg
		ok  bool
		err error
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case dd, ok = <-h.dataChan:
			if !ok {
				return nil //stop data handler by chan closed
			}
			err = dataHandler(dd.Event, dd.EventID, dd.Data)

			if !h.noAck {
				h.ackChan <- eventAck{
					Event:   dd.Event,
					EventID: dd.EventID,
					Err:     err,
				}
			}
		}
	}

	return nil
}
