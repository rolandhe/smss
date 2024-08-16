package conf

import (
	"github.com/spf13/viper"
	"log"
	"time"
)

var Port int
var DefaultIoWriteTimeout time.Duration
var WorkerBuffSize int
var WorkerWaitMsgTimeout time.Duration
var WorkerWaitMsgTimeoutLogSample int64
var MainStorePath string
var MaxLogSize int64

var StoreMaxDays int

var StoreClearInterval int

var DefaultScanSecond int64

var FistExecDelaySecond int64

var WaitFileDeleteLockerTimeout time.Duration

var ServerAliveTimeout time.Duration

var LogPath string

var LogSample int64

var TopicFolderCount uint64

var NoCache bool

var FlushLevel int

var LogRotateMaxSize int
var LogRotateMaxBackups int
var LogRotateMaxAge int

func Init() {
	viper.SetConfigName("config")
	// 设置配置文件类型
	viper.SetConfigType("yaml")
	// 设置配置文件路径，可以设置多个路径
	viper.AddConfigPath("./config")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("fatal error config file: %v", err)
		panic(err)
	}

	Port = viper.GetInt("port")
	DefaultIoWriteTimeout = time.Duration(viper.GetInt64("timeout.net.write")) * time.Millisecond
	ServerAliveTimeout = time.Duration(viper.GetInt64("timeout.server.alive")) * time.Millisecond

	WorkerBuffSize = viper.GetInt("worker.buffSize")
	WorkerWaitMsgTimeout = time.Duration(viper.GetInt64("worker.waitMsgTimeout")) * time.Millisecond
	WorkerWaitMsgTimeoutLogSample = viper.GetInt64("worker.waitMsgTimeoutLogSample")
	MainStorePath = viper.GetString("store.path")
	MaxLogSize = viper.GetInt64("store.maxLogSize")
	StoreMaxDays = viper.GetInt("store.maxDays")
	StoreClearInterval = viper.GetInt("store.clearInterval")
	DefaultScanSecond = viper.GetInt64("background.defaultScanSecond")

	FistExecDelaySecond = viper.GetInt64("background.firstExecSecond")
	WaitFileDeleteLockerTimeout = time.Duration(viper.GetInt64("store.waitDelLockTimeoutMs")) * time.Millisecond

	LogPath = viper.GetString("log.path")
	LogSample = viper.GetInt64("log.sample")

	TopicFolderCount = viper.GetUint64("store.folderCount")
	NoCache = viper.GetBool("store.noCache")

	FlushLevel = viper.GetInt("store.flushLevel")

	LogRotateMaxSize = viper.GetInt("log.rotate.maxSize")
	LogRotateMaxBackups = viper.GetInt("log.rotate.maxBackups")
	LogRotateMaxAge = viper.GetInt("log.rotate.maxAge")
}
