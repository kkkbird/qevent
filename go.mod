module github.com/kkkbird/qevent

go 1.13

require (
	github.com/go-redis/redis/v7 v7.4.0
	github.com/kkkbird/qstream v0.0.0-20200428094322-61fc8d661724
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.6.1
)

//replace github.com/kkkbird/qstream => ../qstream
