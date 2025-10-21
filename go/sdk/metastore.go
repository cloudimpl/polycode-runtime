package sdk

type MetaStore interface {
	CollectionMeta(dataScope DataScope, scopeName string, collection string) (CollectionDescription, error)
	MethodMeta(service string, method string) (MethodDescription, error)
}
