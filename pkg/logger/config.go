package logger

import (
	"bytes"
	"fmt"
	"github.com/rolandhe/smss/conf"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path"
	"runtime"
	"strconv"
)

var core *zap.SugaredLogger

func InitLogger(ppath string) {
	var config zap.Config
	//logPath := ppath

	config = zap.NewDevelopmentConfig()
	config.OutputPaths = []string{ppath}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	isStdout := ppath == "stdout" || ppath == "stderr"

	if isStdout {
		logger, err := config.Build()

		if err != nil {
			panic(err.Error())
		}
		core = logger.Sugar()
		return
	}

	info, err := os.Stat(ppath)
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err.Error())
		}
		if err = os.Mkdir(ppath, os.ModePerm); err != nil {
			panic(err.Error())
		}
	} else if !info.IsDir() {
		panic("log path must be dir")
	}

	// 配置日志滚动
	lumberjackLogger := &lumberjack.Logger{
		Filename:   path.Join(ppath, "out.log"), // 日志文件路径
		MaxSize:    conf.LogRotateMaxSize,       // 每个日志文件的最大尺寸（MB）
		MaxBackups: conf.LogRotateMaxBackups,    // 保留的旧日志文件的最大数量
		MaxAge:     conf.LogRotateMaxAge,        // 保留旧日志文件的最大天数
		Compress:   false,                       // 是否压缩旧的日志文件
		LocalTime:  true,
	}

	// 自定义日志输出方式
	ccore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(config.EncoderConfig),
		zapcore.AddSync(lumberjackLogger),
		config.Level,
	)
	core = zap.New(ccore).Sugar()
}

func Sync() {
	if core != nil {
		core.Sync()
	}
}

//func Get() *zap.SugaredLogger {
//	return core
//}

func getGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func Debugf(template string, args ...interface{}) {
	if conf.LogWithGid {
		nTemplate := fmt.Sprintf("gid=%d,%s", getGoroutineID(), template)
		core.WithOptions(zap.AddCallerSkip(1)).Debugf(nTemplate, args...)
		return
	}
	core.WithOptions(zap.AddCallerSkip(1)).Debugf(template, args...)
}

func Infof(template string, args ...interface{}) {
	if conf.LogWithGid {
		nTemplate := fmt.Sprintf("gid=%d,%s", getGoroutineID(), template)
		core.WithOptions(zap.AddCallerSkip(1)).Infof(nTemplate, args...)
		return
	}
	core.WithOptions(zap.AddCallerSkip(1)).Infof(template, args...)
}

func Warnf(template string, args ...interface{}) {
	if conf.LogWithGid {
		nTemplate := fmt.Sprintf("gid=%d,%s", getGoroutineID(), template)
		core.WithOptions(zap.AddCallerSkip(1)).Warnf(nTemplate, args...)
		return
	}
	core.WithOptions(zap.AddCallerSkip(1)).Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	if conf.LogWithGid {
		nTemplate := fmt.Sprintf("gid=%d,%s", getGoroutineID(), template)
		core.WithOptions(zap.AddCallerSkip(1)).Errorf(nTemplate, args...)
		return
	}
	core.WithOptions(zap.AddCallerSkip(1)).Errorf(template, args...)
}
