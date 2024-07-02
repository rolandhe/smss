package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
)

var core *zap.SugaredLogger

func InitLogger(ppath string) {
	var config zap.Config

	logPath := ppath

	if ppath != "stdout" && ppath != "stderr" {
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
		logPath = path.Join(ppath, "out.log")
	}

	config = zap.NewDevelopmentConfig()
	config.OutputPaths = []string{logPath}

	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := config.Build()

	if err != nil {
		panic(err.Error())
	}
	core = logger.Sugar()
}

func Sync() {
	if core != nil {
		core.Sync()
	}
}

func Get() *zap.SugaredLogger {
	return core
}
