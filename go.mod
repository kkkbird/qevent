module github.com/kkkbird/qevent

go 1.13

require (
	github.com/go-redis/redis/v7 v7.2.0
	github.com/kkkbird/qstream v0.0.0-20200314084153-f2b6b0455cb2
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.5.1
)

//replace github.com/kkkbird/qstream => ../qstream
