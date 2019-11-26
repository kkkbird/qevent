module github.com/kkkbird/qevent

go 1.13

require (
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/kkkbird/qstream v0.0.0-20191126075919-0da45981bced
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/viper v1.5.0
	github.com/stretchr/testify v1.4.0
)

//replace github.com/kkkbird/qstream => ../qstream
