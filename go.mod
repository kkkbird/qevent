module github.com/kkkbird/qevent

go 1.15

require (	
	github.com/go-redis/redis/v8 v8.2.2	
	github.com/kkkbird/qstream v0.0.0-20200927100640-859bb24228a4
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.6.1
	
)

//replace github.com/kkkbird/qstream => ../qstream
