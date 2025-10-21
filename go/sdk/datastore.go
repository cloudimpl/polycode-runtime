package sdk

import (
	"context"
	"time"
)

type WriteConfig struct {
	VersionEquals int64         `json:"version"`
	ExpireIn      time.Duration `json:"expireIn"`
	Unsafe        bool          `json:"unsafe"`
	Upsert        bool          `json:"upsert"`
}

type WriteOption func(*WriteConfig)

func WithExpireIn(expireIn time.Duration) WriteOption {
	return func(cfg *WriteConfig) { cfg.ExpireIn = expireIn }
}

func WithUnsafe() WriteOption {
	return func(cfg *WriteConfig) { cfg.Unsafe = true }
}

func WithUpsert() WriteOption {
	return func(cfg *WriteConfig) { cfg.Upsert = true }
}

type ReadOnlyDataStoreBuilder interface {
	WithTenantId(tenantId string) ReadOnlyDataStoreBuilder
	Get() ReadOnlyDataStore
}

type DataStoreBuilder interface {
	WithTenantId(tenantId string) DataStoreBuilder
	Get() DataStore
}

type ReadOnlyDataStore interface {
	ServiceCollection(name string) ReadOnlyCollection
	AppCollection(name string) ReadOnlyCollection
}

type DataStore interface {
	ServiceCollection(name string) Collection
	AppCollection(name string) Collection
}

type ReadOnlyCollection interface {
	GetOne(id string) (ReadOnlyDoc, error)
	Query() ReadOnlyQuery

	Path() string
}

type Collection interface {
	GetOne(id string) (Doc, error)
	Query() Query
	InsertOne(id string, item interface{}, opts ...WriteOption) (Doc, error)

	Path() string
}

type ReadOnlyDoc interface {
	ChildCollection(name string) ReadOnlyCollection

	Path() string
	Unmarshal(item interface{}) error
}

type Doc interface {
	ExpireIn(expireIn time.Duration, opts ...WriteOption) error
	Update(item interface{}, opts ...WriteOption) error
	Delete(opts ...WriteOption) error
	ChildCollection(name string) Collection

	Path() string
	Unmarshal(item interface{}) error
}

type ReadOnlyQuery interface {
	Filter(expr string, args ...interface{}) ReadOnlyQuery
	Limit(limit int) ReadOnlyQuery
	GetOne(ctx context.Context) (ReadOnlyDoc, error)
	GetAll(ctx context.Context) ([]ReadOnlyDoc, error)

	// Optional future support
	// Iter() PagingIter
	// AllWithNextToken(ctx context.Context, ret interface{}) (PageToken, error)
	// Count(ctx context.Context) (int, error)
}

type Query interface {
	Filter(expr string, args ...interface{}) Query
	Limit(limit int) Query
	GetOne(ctx context.Context) (Doc, error)
	GetAll(ctx context.Context) ([]Doc, error)

	// Optional future support
	// Iter() PagingIter
	// AllWithNextToken(ctx context.Context, ret interface{}) (PageToken, error)
	// Count(ctx context.Context) (int, error)
}
