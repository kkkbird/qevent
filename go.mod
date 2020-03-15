module github.com/kkkbird/qevent

go 1.13

require (
	github.com/go-redis/redis/v7 v7.2.0
	github.com/golang/protobuf v1.3.5 // indirect
	github.com/kkkbird/qstream v0.0.0-20200315140256-17a4e13501c2
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.5.1
)

//replace github.com/kkkbird/qstream => ../qstream
