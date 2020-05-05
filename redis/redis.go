package redis

import (
	"github.com/garyburd/redigo/redis"
	"github.com/sirupsen/logrus"
	"log"
)
func RedisConnect() (redis.Conn) {
	option := redis.DialPassword("syf93529")
	conn,err := redis.Dial("tcp", "127.0.0.1:6379", option)
	if err != nil {
		log.Fatal("redis connect failed")
		return nil
	} else {
		logrus.Infoln("redis connect success")
	}
	defer conn.Close()

	return conn
}