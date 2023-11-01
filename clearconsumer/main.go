package main

import (
	"context"
	"time"

	"github.com/kkkbird/qapp"
	"github.com/redis/go-redis/v9"

	_ "github.com/kkkbird/qlog"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	redisDB     *redis.Client
	maxIdle     time.Duration
	streamInfos []StreamInfo

	log = logrus.WithField("app", "clearconsumer")
)

func initRedis(ctx context.Context) (qapp.CleanFunc, error) {
	setting := "redis"
	opts := &redis.Options{
		Addr:                  viper.GetString(setting + ".addr"),
		Password:              viper.GetString(setting + ".password"),
		PoolSize:              viper.GetInt(setting + ".poolsize"),
		MinIdleConns:          viper.GetInt(setting + ".minidleconns"),
		DB:                    viper.GetInt(setting + ".db"),
		ContextTimeoutEnabled: true,
	}

	log.Debugf("open redis(%s), addr=%s, db=%d, poolsize=%d, minidleconns=%d",
		setting, opts.Addr, opts.DB, opts.PoolSize, opts.MinIdleConns)

	redisDB = redis.NewClient(opts)

	_, err := redisDB.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) {
		redisDB.Close()
	}, nil
}

// 载入配置文件
func initApp(ctx context.Context) (qapp.CleanFunc, error) {
	var err error
	streamInfos, err = readStreamInfo()
	if err != nil {
		return nil, err
	}

	maxIdle, err = time.ParseDuration(viper.GetString("maxidle"))

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func runApp(ctx context.Context) error {
	var err error
	for _, info := range streamInfos {
		if err = info.Clear(redisDB); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	qapp.New("clearcosumer").
		AddInitStage("init_redis", initRedis).
		AddInitStage("init_app", initApp).
		AddDaemons(runApp).
		Run()
}
