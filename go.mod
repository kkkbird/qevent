module github.com/kkkbird/qevent

go 1.15

require (
	github.com/go-redis/redis/v8 v8.7.1
	github.com/kkkbird/qapp v0.0.0-20201221094717-509fb018ad18
	github.com/kkkbird/qlog v0.0.0-20201217120852-62b87cd08f16
	github.com/kkkbird/qstream v0.0.0-20200927100640-859bb24228a4
	github.com/lestrrat-go/strftime v1.0.4 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0

)

// replace github.com/kkkbird/qstream => ../qstream
// replace github.com/go-redis/redis/v8 => ../redis
