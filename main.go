package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const HANDLE_DIG = " /dig?"

type cmdParams struct {
	logFilePath  string
	goroutineNum int
	l            string
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type urlData struct {
	data  digData
	uid   string
	uNode urlNode
}

type urlNode struct {
	unType string
	unRid  int
	unUrl  string
	unTime string
}

type storageBlock struct {
	counterType  string
	storageModel string
	unode        urlNode
}

var log = logrus.New()
var pool *redis.Pool
func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
	option := redis.DialPassword("syf93529")
	pool = &redis.Pool{
		MaxIdle:     25,   //最大空闲链接数
		MaxActive:   0,   // 表示和数据库的最大链接数， 0 表示没有限制
		IdleTimeout: 100, // 最大空闲时间
		Dial: func() (redis.Conn, error) { // 初始化链接的代码， 链接哪个ip的redis
			return redis.Dial("tcp", "localhost:6379", option)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				return fmt.Errorf("ping redis error: %s", err)
			}
			return nil
		},
	}
}
var lock sync.Mutex
func main() {
	//pool := &redis.Pool{
	//	MaxIdle:     25,   //最大空闲链接数
	//	MaxActive:   0,   // 表示和数据库的最大链接数， 0 表示没有限制
	//	IdleTimeout: 100, // 最大空闲时间
	//	Dial: func() (redis.Conn, error) { // 初始化链接的代码， 链接哪个ip的redis
	//		conn, err := redis.Dial("tcp", "localhost:6379")
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		//验证redis密码
	//		if _, authErr := conn.Do("AUTH", "syf93529"); authErr != nil {
	//			return nil, fmt.Errorf("redis auth password error: %s", authErr)
	//		}
	//		return conn, err
	//	},
	//	TestOnBorrow: func(c redis.Conn, t time.Time) error {
	//		_, err := c.Do("PING")
	//		if err != nil {
	//			return fmt.Errorf("ping redis error: %s", err)
	//		}
	//		return nil
	//	},
	//}
	redisPool := pool.Get()
	defer redisPool.Close()
	// 获取参数
	logFilePath := flag.String("logFilePath", "/usr/local/nginx/logs/dig.log", "log file path")
	goroutineNum := flag.Int("goroutineNum", 5, "go routine")
	l := flag.String("l", "/tmp/log", "tmp log")
	flag.Parse()

	params := cmdParams{*logFilePath, *goroutineNum, *l}

	// 打印日志
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_WRONLY, 0777)
	if err == nil {
		log.Out = logFd
		defer logFd.Close()
	}
	log.Infoln("Execute start...")
	log.Infof("Params: logFilePath=%s, goroutineNum=%d", params.logFilePath, params.goroutineNum)

	// 初始化一些channel，用于数据传递
	logChannel := make(chan string, params.goroutineNum*3)
	pvChannel := make(chan urlData, params.goroutineNum)
	uvChannel := make(chan urlData, params.goroutineNum)
	storageChannel := make(chan storageBlock, params.goroutineNum)

	// 日子消费者
	go readFileLineByLine(params, logChannel)

	// 创建一组日志处理
	for i := 0; i <= params.goroutineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	// 创建uv pv 统计器 此部分功能可拓展
	go pvCount(pvChannel, storageChannel)
	go uvCount(uvChannel, storageChannel, redisPool)

	go dataStorage(storageChannel, redisPool)
	time.Sleep(time.Second * 5000)
}

// 数据存储
func dataStorage(storageChannel chan storageBlock, redis redis.Conn) {
	for block := range storageChannel {
		prefix := block.counterType + "_"
		setKeys := []string{
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" + getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" + getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" + getTime(block.unode.unTime, "min"),
		}

		for _, key := range setKeys {
			lock.Lock()
			_, err := redis.Do(block.storageModel, key, 1, block.unode.unRid)
			lock.Unlock()
			if err != nil {
				log.Errorln("dataStorage: redis storage error, ", err.Error())
			}
		}
	}
}

// pv统计
func pvCount(pvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range pvChannel {
		sItem := storageBlock{"pv", "ZINCRBY", data.uNode}
		storageChannel <- sItem
	}
}

// uv统计
func uvCount(uvChannel chan urlData, storageChannel chan storageBlock, redis redis.Conn) {
	for data := range uvChannel {
		hyperLogKey := "uv_hpll" + getTime(data.data.time, "day")
		lock.Lock()
		_, err := redis.Do("PFADD", hyperLogKey, data.uid, "EX", 86400)
		lock.Unlock()
		if err != nil {
			log.Warningln("uvCount: check redis hyperLog failed, ", err.Error())
		}

		//if ret != 1 {
		//	continue
		//}
		sItem := storageBlock{"uv", "ZINCRBY", data.uNode}
		storageChannel <- sItem
	}
}

// 格式化url
func formatUrl(url, time string) urlNode {
	pagePosition := strings.Index(url, "=")
	if pagePosition != -1 { // 文章列表
		page := string([]rune(url)[pagePosition+1:])
		currentPage, _ := strconv.Atoi(page)
		return urlNode{"list", currentPage, url, time}
	} else {
		DetailIdPosition := strings.Index(url, "es/")
		if DetailIdPosition != -1 {
			articleId := string([]rune(url)[DetailIdPosition+3:])
			articleDetailId, _ := strconv.Atoi(articleId)
			return urlNode{"detail", articleDetailId, url, time}
		} else {
			return urlNode{"home", 1, url, time}
		}
	}
}

// log消费
func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) {
	for logStr := range logChannel {
		// 切割日子字符串，抠出打点上报的日志
		data := cutLogFetchData(logStr)

		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		uData := urlData{data, uid, formatUrl(data.url, data.time)}
		pvChannel <- uData
		uvChannel <- uData
	}
}

// 切割日志
func cutLogFetchData(logStr string) digData {
	logStr = strings.TrimSpace(logStr)
	pos1 := str.IndexOf(logStr, HANDLE_DIG, 0)

	if pos1 == -1 {
		return digData{}
	}
	pos1 = pos1 + len(HANDLE_DIG)
	pos2 := str.IndexOf(logStr, " HTTP/", 0)
	if (pos2 == -1) {
		return digData{}
	}

	d := str.Substr(logStr, pos1, pos2-pos1)

	urlInfo, err := url.Parse("http://localhost/?" + d)
	if err != nil {
		return digData{}
	}
	data := urlInfo.Query()

	return digData{
		data.Get("time"),
		data.Get("url"),
		data.Get("refer"),
		data.Get("ua"),
	}

}

// 格式化时间
func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
		break
	case "hour":
		item = "2006-01-02 15"
		break
	case "min":
		item = "2006-01-02 15:04"
		break
	default:
		break

	}

	t, _ := time.Parse(item, time.Now().Format(item))
	return strconv.FormatInt(t.Unix(), 10)
}

// 读取日志
func readFileLineByLine(params cmdParams, logChannel chan string) error {
	fd, err := os.Open(params.logFilePath)
	if err != nil {
		log.Error("readFileLineByLine: can't open log file")
	}
	defer fd.Close()
	bufferRead := bufio.NewReader(fd)
	count := 0
	for {
		line, err := bufferRead.ReadString('\n')
		logChannel <- line
		count++
		if count%(params.goroutineNum*1000) == 0 {
			log.Infof("readFileLineByLine: line: %d", count)
		}

		if err != nil {
			if (err == io.EOF) {
				time.Sleep(time.Second * 3)
				log.Info("readFileLineByLine: wait 3 second")
			} else {
				log.Warning("readFileLineByLine: read log error")
			}
		}
	}

	return nil
}
