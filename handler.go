package qevent

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/kkkbird/qstream"
	"github.com/sirupsen/logrus"
)

var (
	log = logrus.WithField("pkg", "qevent")
)

var (
	defaultWorkCount       = 5
	defaultGroupStartID    = "0-0"
	maxEventClaimCount     = 10
	waitFreeWorkerInterval = 10 * time.Millisecond
	readEventBlockInterval = 10 * time.Millisecond
	ErrCloseTimeout        = errors.New("Close timeout")
	ErrMessageTrimmed      = errors.New("data trimmed")
)

type Handler struct {
	client               *redis.Client
	codec                qstream.DataCodec
	events               []string
	groupName            string
	groupStartID         string
	consumerName         string
	workerCount          int
	dataChan             chan eventMsg
	ackChan              chan eventAck
	closeTimeoutDuration time.Duration
	noAck                bool
	ignoreTrimmedData    bool
	checkPendingDuration time.Duration // == 0 means do not check pending
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

func WithNoAck() HandlerOpts {
	return func(h *Handler) {
		h.noAck = true
	}
}

func WithCheckPending(d time.Duration) HandlerOpts {
	return func(h *Handler) {
		h.checkPendingDuration = d
	}
}

func WithIgnoreTrimmedData() HandlerOpts {
	return func(h *Handler) {
		h.ignoreTrimmedData = true
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
		closeTimeoutDuration: 0,     // 0 means never timeout
		noAck:                false, // enable ack by default
	}

	for _, o := range opts {
		o(h)
	}

	h.dataChan = make(chan eventMsg) // workerCount may be changed in opts
	h.ackChan = make(chan eventAck, h.workerCount)

	return h
}

func (h *Handler) Run(ctx context.Context, dataHandler DataHandler, events ...string) error {
	h.events = events
	// run handler
	go func() {
		var wg sync.WaitGroup
		wg.Add(h.workerCount)

		for i := 0; i < h.workerCount; i++ {
			go func() {
				defer wg.Done()

				var err error

				for dd := range h.dataChan {
					if dd.Data != nil || !h.ignoreTrimmedData {
						err = dataHandler(dd.Event, dd.EventID, dd.Data)
					}

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
	return h.reader(ctx)
}

func deltaStreamId(streamID string, deltaFirst int64, deltaSecond int64) string {
	msgId := strings.Split(streamID, "-")

	oFirst, _ := strconv.ParseInt(msgId[0], 10, 64)
	oSecond, _ := strconv.ParseInt(msgId[1], 10, 64)

	return strconv.FormatInt(oFirst+deltaFirst, 10) + "-" + strconv.FormatInt(oSecond+deltaSecond, 10)
}

func nowStreamId() string {
	return strconv.FormatInt(int64(time.Now().UnixNano()/int64(time.Millisecond)), 10) + "-0"
}

func (h *Handler) checkPending(lastPendingIDs []string) (map[string][]qstream.StreamSubResult, error) {
	var (
		startId  string
		endId    string
		claimIds = make([]string, 0)
	)

	for i, evt := range h.events {
		if lastPendingIDs[i] != "-" {
			startId = deltaStreamId(lastPendingIDs[i], 0, 1)
		} else {
			startId = "-"
		}
		endId = deltaStreamId(nowStreamId(), -int64(h.checkPendingDuration/time.Millisecond), 0)

		pending, err := h.client.XPendingExt(&redis.XPendingExtArgs{
			Stream: evt,
			Group:  h.groupName,
			Start:  startId,
			End:    endId,
			Count:  int64(h.workerCount),
		}).Result()

		if err != nil {
			return nil, err
		}

		if len(pending) == 0 {
			continue
		}

		for _, p := range pending {
			if p.Idle > h.checkPendingDuration { // normally, we donot need to check the duration again here
				if p.RetryCount >= int64(maxEventClaimCount) {
					// TODO: too many retry, should handle error, now do nothing
				}

				if p.Consumer == h.consumerName {
					// TODO: already owned by current consumer !! should handle error
					continue
				}

				claimIds = append(claimIds, p.ID)
			}
		}

		if len(claimIds) > 0 {
			var (
				rlt []redis.XMessage
				err error
			)

			// TODO: change back after this bug fixed: https://github.com/go-redis/redis/issues/1202
			// 2020/3/14, bug still remain on v7.2.0, but pool.go will remove bad connection and following call will be ok
			if false {
				lastPendingIDs[i] = claimIds[len(claimIds)-1]

				rlt, err = h.client.XClaim(&redis.XClaimArgs{
					Stream:   evt,
					Group:    h.groupName,
					Consumer: h.consumerName,
					MinIdle:  h.checkPendingDuration,
					Messages: claimIds,
				}).Result()

				if err != nil {
					return nil, err
				}
			} else {
				// handle claimId one by one
				rlt = make([]redis.XMessage, 0)
				for _, cid := range claimIds {
					lastPendingIDs[i] = cid
					rltTmp, err := h.client.XClaim(&redis.XClaimArgs{
						Stream:   evt,
						Group:    h.groupName,
						Consumer: h.consumerName,
						MinIdle:  h.checkPendingDuration,
						Messages: []string{cid},
					}).Result()

					if err == redis.Nil {
						h.client.XAck(evt, h.groupName, cid) // TODO: message has been trimmed, ack directly now, may add code to notify handler some messages are trimmed
					} else if err != nil {
						return nil, err
					} else {
						rlt = append(rlt, rltTmp...)
					}
				}
			}

			claimedMsg := make(map[string][]qstream.StreamSubResult)

			if len(rlt) > 0 { // len(rlt) may == 0 because other consumer may claim these pending message, will return an empty map
				subRlt, err := qstream.XMessage2Data(rlt, h.codec)

				if err != nil {
					return nil, err
				}

				claimedMsg[evt] = subRlt
			}

			return claimedMsg, nil
		}
	}

	return nil, redis.Nil
}

func (h *Handler) reader(ctx context.Context) error {
	var (
		first     eventMsg
		transChan chan eventMsg
		doneChan  <-chan struct{}
		nowTs     int64
		err       error
		result    map[string][]qstream.StreamSubResult
	)

	forceCloseCtx := context.Background()
	eventArray := make([]eventMsg, 0, h.workerCount*2)
	doneChan = ctx.Done()

	sub := qstream.NewRedisStreamGroupSub(h.client, h.codec, h.groupName, h.groupStartID, h.consumerName, h.noAck, h.events...)
	lastIDs := make([]string, len(h.events))
	for i := 0; i < len(h.events); i++ {
		lastIDs[i] = "0-0"
	}

	lastCheckPendingTs := time.Now().UnixNano() // set to now because we do not want check pending at start
	lastPendingIDs := make([]string, len(h.events))

	for i := 0; i < len(h.events); i++ {
		lastPendingIDs[i] = "-"
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
				var cancel context.CancelFunc
				forceCloseCtx, cancel = context.WithTimeout(forceCloseCtx, h.closeTimeoutDuration)
				defer cancel()
			}
		case <-forceCloseCtx.Done():
			return ErrCloseTimeout
		case transChan <- first:
			eventArray = eventArray[1:]
		case ack := <-h.ackChan:
			if !h.noAck {
				sub.Ack(ack.Event, ack.EventID) // TODO: we may check error to handle ack later
			}
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
						if !h.noAck {
							sub.Ack(ack.Event, ack.EventID)
						}
					}
				}
			}

			nowTs = time.Now().UnixNano()

			if h.checkPendingDuration > 0 && nowTs-lastCheckPendingTs > int64(h.checkPendingDuration) {
				result, err = h.checkPending(lastPendingIDs)

				if err == redis.Nil { // only update lastCheckPendingTs when no pending, otherwise we should check pending in next round
					for i := 0; i < len(h.events); i++ {
						lastPendingIDs[i] = "-"
					}
					lastCheckPendingTs = nowTs
				}
			} else {
				result, err = sub.Read(int64(h.workerCount), readEventBlockInterval, lastIDs...) // won't return redis.Nil if checkPending

				if err == redis.Nil {
					continue
				}
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
					}
					if lastIDs[idx] != ">" {
						lastIDs[idx] = v[len(v)-1].StreamID
					}
				}
			}
		}
	}
}
