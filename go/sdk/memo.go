package sdk

type Memo interface {
	Get() (Response, error)
}
