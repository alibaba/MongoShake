module github.com/alibaba/MongoShake/v2

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/getlantern/deepcopy v0.0.0-20160317154340-7f45deb8130a
	github.com/gugemichael/nimo4go v0.0.0-20200403101749-647883f3a053
	github.com/nightlyone/lockfile v1.0.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.7.0
	github.com/vinllen/go-diskqueue v1.0.1
	github.com/vinllen/log4go v0.0.0-20180514124125-3848a366df9d
	github.com/vinllen/mgo v0.0.0-20200603094852-6e8a76964ea9
	go.mongodb.org/mongo-driver v1.4.5
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

replace github.com/vinllen/go-diskqueue => github.com/nanmu42/go-diskqueue v1.0.2-0.20210121041308-a95eac02055a
