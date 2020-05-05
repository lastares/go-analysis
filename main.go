package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const HANDLE_DIG = " /dig?"
const HANDLE_ARTICLE_PAGE = "page="
const HANDLE_ARTICLE_DETAIL = "/articles/"

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

func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
}

func main() {
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
	for i := 0; i <= params.goroutineNum; {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	// 创建uv pv 统计器 此部分功能可拓展
	go pvCount(pvChannel, storageChannel)
	go uvCount(uvChannel, storageChannel)

	go dataStorage(storageChannel)
	time.Sleep(time.Second * 1000)
}

func dataStorage(storageChannel chan storageBlock) {

}

func pvCount(pvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range pvChannel {
		sItem := storageBlock{"pv", "ZINCREBY", data.uNode}
		storageChannel <- sItem
	}
}

func formatUrl(url, time string) urlNode {
	pagePosition := strings.Index(url, "=")
	if pagePosition != -1 { // 文章列表
		page := string([]rune(url)[pagePosition+1:])
		log.Infof("文章列表page: %s", page)
		currentPage, _ := strconv.Atoi(page)
		return urlNode{"list", currentPage, url, time}
	} else {
		DetailIdPosition := strings.Index(url, "es/")
		if DetailIdPosition != -1 {
			articleId := string([]rune(url)[DetailIdPosition+3:])
			log.Infof("文章详情articleId: %s", articleId)
			articleDetailId, _ := strconv.Atoi(articleId)
			return urlNode{"detail", articleDetailId, url, time}
		} else {
			log.Info("文章首页: 0")
			return urlNode{"detail", 0, url, time}
		}
	}
}

func uvCount(uvChannel chan urlData, storageChannel chan storageBlock) {

}

func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) {
	for logStr := range logChannel {
		// 切割日子字符串，抠出打点上报的日志
		data := cutLogFetchData(logStr)

		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		uData := urlData{data, uid, formatUrl(data.url, data.time)}
		log.Infoln(uData)
		pvChannel <- uData
		uvChannel <- uData
	}
}

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

/**
* 格式化时间
 */
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
		log.Infof("line: %s", line)
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
