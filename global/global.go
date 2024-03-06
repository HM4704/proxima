package global

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lines"
	"github.com/lunfardo314/proxima/util/set"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Global struct {
	*zap.SugaredLogger
	ctx            context.Context
	stopFun        context.CancelFunc
	logStopOnce    *sync.Once
	stopOnce       *sync.Once
	mutex          sync.RWMutex
	components     set.Set[string]
	enabledTrace   atomic.Bool
	traceTagsMutex sync.RWMutex
	traceTags      set.Set[string]
}

const TraceTag = "global"

func NewFromConfig() *Global {
	lvlStr := viper.GetString("logger.level")
	lvl := zapcore.InfoLevel
	if lvlStr != "" {
		var err error
		lvl, err = zapcore.ParseLevel(lvlStr)
		util.AssertNoError(err)
	}

	output := []string{"stderr"}
	out := viper.GetString("logger.output")
	if out != "" {
		output = append(output, out)
	}
	return _new(lvl, output)
}

func NewDefault() *Global {
	return _new(zapcore.DebugLevel, []string{"stderr"})
}

func _new(logLevel zapcore.Level, outputs []string) *Global {
	ctx, cancelFun := context.WithCancel(context.Background())
	return &Global{
		ctx:           ctx,
		stopFun:       cancelFun,
		SugaredLogger: NewLogger("", logLevel, outputs, ""),
		traceTags:     set.New[string](),
		stopOnce:      &sync.Once{},
		logStopOnce:   &sync.Once{},
		components:    set.New[string](),
	}
}

func (l *Global) MarkWorkProcessStarted(name string) {
	l.Tracef(TraceTag, "MarkWorkProcessStarted: %s", name)
	l.mutex.Lock()
	defer l.mutex.Unlock()

	util.Assertf(!l.components.Contains(name), "global: repeating work-process %s", name)
	l.components.Insert(name)
}

func (l *Global) MarkWorkProcessStopped(name string) {
	l.Tracef(TraceTag, "MarkWorkProcessStopped: %s", name)
	l.mutex.Lock()
	defer l.mutex.Unlock()

	util.Assertf(l.components.Contains(name), "global: unknown component %s", name)
	l.components.Remove(name)
}

func (l *Global) Stop() {
	l.Tracef(TraceTag, "Stop")
	l.stopOnce.Do(func() {
		l.Log().Info("global STOP invoked..")
		l.stopFun()
	})
}

func (l *Global) Ctx() context.Context {
	return l.ctx
}

func (l *Global) _withRLock(fun func()) {
	l.mutex.RLock()
	fun()
	l.mutex.RUnlock()
}

func (l *Global) MustWaitAllWorkProcessesStop(timeout ...time.Duration) {
	l.Tracef(TraceTag, "MustWaitAllWorkProcessesStop")

	deadline := time.Now().Add(math.MaxInt)
	if len(timeout) > 0 {
		deadline = time.Now().Add(timeout[0])
	}
	exit := false
	for {
		l._withRLock(func() {
			if len(l.components) == 0 {
				l.logStopOnce.Do(func() {
					l.Log().Info("all work processes stopped")
				})
				exit = true
			}
		})
		if exit {
			return
		}
		time.Sleep(5 * time.Millisecond)
		if time.Now().After(deadline) {
			l._withRLock(func() {
				ln := lines.New()
				for s := range l.components {
					ln.Add(s)
				}
				l.Log().Errorf("MustWaitAllWorkProcessesStop: exceeded timeout. Still running components: %s", ln.Join(","))
			})
			return
		}
	}
}

func (l *Global) TraceLog(log *zap.SugaredLogger, tag string, format string, args ...any) {
	if !l.enabledTrace.Load() {
		return
	}

	l.traceTagsMutex.RLock()
	defer l.traceTagsMutex.RUnlock()

	for _, t := range strings.Split(tag, ",") {
		if l.traceTags.Contains(t) {
			log.Infof("TRACE(%s) %s", t, fmt.Sprintf(format, util.EvalLazyArgs(args...)...))
			return
		}
	}
}

func (l *Global) Log() *zap.SugaredLogger {
	return l.SugaredLogger
}

func (l *Global) Tracef(tag string, format string, args ...any) {
	l.TraceLog(l.Log(), tag, format, args...)
}

func (l *Global) Assertf(cond bool, format string, args ...any) {
	if !cond {
		l.SugaredLogger.Fatalf("assertion failed:: "+format, util.EvalLazyArgs(args...)...)
	}
}

func (l *Global) AssertNoError(err error, prefix ...string) {
	if err != nil {
		pref := "error: "
		if len(prefix) > 0 {
			pref = strings.Join(prefix, " ") + ": "
		}
		l.SugaredLogger.Fatalf(pref+"%w", err)
	}
}

func (l *Global) AssertMustError(err error) {
	if err == nil {
		l.SugaredLogger.Panicf("AssertMustError: error expected")
	}
}

func (l *Global) EnableTrace(enable bool) {
	l.enabledTrace.Store(enable)
}

func (l *Global) EnableTraceTags(tags ...string) {
	func() {
		l.traceTagsMutex.Lock()
		defer l.traceTagsMutex.Unlock()

		for _, t := range tags {
			st := strings.Split(t, ",")
			for _, t1 := range st {
				l.traceTags.Insert(strings.TrimSpace(t1))
			}
			l.enabledTrace.Store(true)
		}
	}()

	for _, tag := range tags {
		l.Tracef(tag, "trace tag enabled")
	}
}

func (l *Global) DisableTraceTag(tag string) {
	l.traceTagsMutex.Lock()
	defer l.traceTagsMutex.Unlock()

	l.traceTags.Remove(tag)
	if len(l.traceTags) == 0 {
		l.enabledTrace.Store(true)
	}
}
