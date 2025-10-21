package sdk

type LogEntry interface {
	Str(key string, val string) LogEntry
	Int64(key string, val int64) LogEntry
	Float64(key string, val float64) LogEntry
	Bool(key string, val bool) LogEntry
	Done()
	Msg(msg string)
}

type Logger interface {
	Debug() LogEntry
	Info() LogEntry
	Warn() LogEntry
	Error() LogEntry
}
