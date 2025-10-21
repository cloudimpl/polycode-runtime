package runtime

import (
	"encoding/json"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"time"
)

const (
	DebugLevel sdk.LogLevel = "DEBUG"
	InfoLevel  sdk.LogLevel = "INFO"
	WarnLevel  sdk.LogLevel = "WARN"
	ErrorLevel sdk.LogLevel = "ERROR"
)

type LogMsg struct {
	Level     sdk.LogLevel           `json:"level"`
	Section   string                 `json:"section"`
	Tags      map[string]interface{} `json:"tags"`
	Timestamp int64                  `json:"timestamp"`
	Message   string                 `json:"message"`
}

type LogEntry struct {
	msg *LogMsg
}

func (entry *LogEntry) Str(key string, val string) sdk.LogEntry {
	entry.msg.Tags[key] = val
	return entry
}

func (entry *LogEntry) Int64(key string, val int64) sdk.LogEntry {
	entry.msg.Tags[key] = val
	return entry
}

func (entry *LogEntry) Float64(key string, val float64) sdk.LogEntry {
	entry.msg.Tags[key] = val
	return entry
}

func (entry *LogEntry) Bool(key string, val bool) sdk.LogEntry {
	entry.msg.Tags[key] = val
	return entry
}

func (entry *LogEntry) Done() {
	entry.msg.Timestamp = time.Now().UnixMicro()
	logJson, err := json.Marshal(entry.msg)
	if err == nil {
		println(string(logJson))
	}
}

func (entry *LogEntry) Msg(msg string) {
	entry.msg.Message = msg
	entry.Done()
}

type JsonLogger struct {
	section string
}

func (logger *JsonLogger) Debug() sdk.LogEntry {
	return logger.Log(DebugLevel)
}

func (logger *JsonLogger) Info() sdk.LogEntry {
	return logger.Log(InfoLevel)
}

func (logger *JsonLogger) Warn() sdk.LogEntry {
	return logger.Log(WarnLevel)
}

func (logger *JsonLogger) Error() sdk.LogEntry {
	return logger.Log(ErrorLevel)
}

func (logger *JsonLogger) Log(level sdk.LogLevel) sdk.LogEntry {
	return &LogEntry{
		msg: &LogMsg{
			Level:   level,
			Section: logger.section,
			Tags:    make(map[string]interface{}),
		},
	}
}
