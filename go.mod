module github.com/kkkbird/qevent

go 1.13

require (
	github.com/go-redis/redis/v7 v7.2.0
	github.com/golang/protobuf v1.3.5 // indirect
	github.com/kkkbird/qstream v0.0.0-20200324110053-25377cb8597f
	github.com/sirupsen/logrus v1.5.0
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.5.1
	golang.org/x/net v0.0.0-20200320220750-118fecf932d8 // indirect
)

//replace github.com/kkkbird/qstream => ../qstream
