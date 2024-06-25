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
var MainStorePath string
var MaxLogSize int64

var MqBufferSize int

var StoreMaxDays int

var StoreClearInterval int

var LifeDefaultScanSec int64

func Init() error {
	viper.SetConfigName("config")
	// 设置配置文件类型
	viper.SetConfigType("yaml")
	// 设置配置文件路径，可以设置多个路径
	viper.AddConfigPath("./config")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("fatal error config file: %v\n", err)
		return err
	}

	Port = viper.GetInt("port")
	DefaultIoWriteTimeout = time.Duration(viper.GetInt64("net.writeTimeout")) * time.Second
	WorkerBuffSize = viper.GetInt("worker.buffSize")
	WorkerWaitMsgTimeout = time.Duration(viper.GetInt64("worker.waitTimeoutMs")) * time.Millisecond
	MainStorePath = viper.GetString("store.path")
	MaxLogSize = viper.GetInt64("store.maxLogSize")
	MqBufferSize = viper.GetInt("mq.buffSize")
	StoreMaxDays = viper.GetInt("store.maxDays")
	StoreClearInterval = viper.GetInt("store.clearInterval")
	LifeDefaultScanSec = viper.GetInt64("life.defaultScanSec")
	return nil
}
