package main

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
)

type StreamInfo struct {
	Name string
}

func (si *StreamInfo) Clear(client *redis.Client) error {
	ctx := context.Background()

	groups, err := client.XInfoGroups(ctx, si.Name).Result()

	if err != nil {
		log.Errorf("get (%s) xinfo groups fail:%v", si.Name, err)
		return err
	}

	log.Infof("STREAM (%s) has %d groups", si.Name, len(groups))

	for _, g := range groups {
		if g.Consumers == 0 {
			log.Infof("GROUP (%s) has no consumer", g.Name)
			continue
		}
		log.Infof("GROUP (%s) has %d consumer, pending %d", g.Name, g.Consumers, g.Pending)

		consumers, err := client.XInfoConsumers(ctx, si.Name, g.Name).Result()
		if err != nil {
			log.Errorf("get (%s) xinfo consumer fail(%s):%v", si.Name, g, err)
			return err
		}

		var removed int
		for _, c := range consumers {
			if c.Idle > maxIdle.Milliseconds() {
				if c.Pending > 0 {
					log.Warnf("consumer (%s) as %d pending message", c.Name, c.Pending)
					continue
				}

				err = client.XGroupDelConsumer(ctx, si.Name, g.Name, c.Name).Err()
				if err != nil {
					log.Warnf("consumer (%s) remove fail:%s", c.Name, err)
					continue
				}
				removed++
			}
		}
		log.Infof("removed %d consumer", removed)
	}

	return nil
}

func readStreamInfo() ([]StreamInfo, error) {
	stream := viper.GetStringSlice("stream")

	sis := make([]StreamInfo, len(stream))

	for i := range stream {
		sis[i].Name = stream[i]
	}

	log.Infof("read stream info: %+v", sis)

	return sis, nil
}
