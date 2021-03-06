package appraft

import (
	"sync"

	dlogger "github.com/lni/dragonboat/v3/logger"
	"go.uber.org/zap"
)

type (
	LoggerFactory struct {
		mux    sync.Mutex
		logger *zap.Logger
		pkgs   map[string]*Logger
	}
)

func (loggerFactory *LoggerFactory) Create(pkgName string) dlogger.ILogger {
	loggerFactory.mux.Lock()
	defer loggerFactory.mux.Unlock()
	if val, ok := loggerFactory.pkgs[pkgName]; ok {
		return val
	}
	atomicLevel := zap.NewAtomicLevel()

	loggerFactory.pkgs[pkgName] = &Logger{
		SugaredLogger: loggerFactory.logger.Named(pkgName).WithOptions(zap.IncreaseLevel(atomicLevel)).Sugar(),
		atomicLevel:   atomicLevel,
	}
	return loggerFactory.pkgs[pkgName]
}

func NewLoggerFactory(logger *zap.Logger) dlogger.Factory {
	loggerFactory := &LoggerFactory{
		logger: logger,
		pkgs:   make(map[string]*Logger),
	}
	return loggerFactory.Create
}

func SetLoggerFactory(logger *zap.Logger) {
	dlogger.SetLoggerFactory(NewLoggerFactory(logger))
}
