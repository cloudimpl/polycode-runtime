package runtime

import (
	"context"
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
	"time"
)

type ReadOnlyDataStoreBuilder struct {
	client        ServiceClient
	sessionId     string
	modelRegistry *ModelRegistry

	tenantId string
}

func (f *ReadOnlyDataStoreBuilder) WithTenantId(tenantId string) sdk.ReadOnlyDataStoreBuilder {
	f.tenantId = tenantId
	return f
}

func (f *ReadOnlyDataStoreBuilder) Get() sdk.ReadOnlyDataStore {
	fmt.Printf("getting read only db for tenant id = %s", f.tenantId)
	return &ReadOnlyDataStore{
		client:        f.client,
		sessionId:     f.sessionId,
		modelRegistry: f.modelRegistry,

		tenantId: f.tenantId,
	}
}

var _ sdk.ReadOnlyDataStoreBuilder = (*ReadOnlyDataStoreBuilder)(nil)

type DataStoreBuilder struct {
	client        ServiceClient
	sessionId     string
	modelRegistry *ModelRegistry

	tenantId string
}

func (f *DataStoreBuilder) WithTenantId(tenantId string) sdk.DataStoreBuilder {
	f.tenantId = tenantId
	return f
}

func (f *DataStoreBuilder) Get() sdk.DataStore {
	fmt.Printf("getting db for tenant id = %s", f.tenantId)
	return &DataStore{
		client:        f.client,
		sessionId:     f.sessionId,
		modelRegistry: f.modelRegistry,

		tenantId: f.tenantId,
	}
}

var _ sdk.DataStoreBuilder = (*DataStoreBuilder)(nil)

type ReadOnlyDataStore struct {
	client    ServiceClient
	sessionId string
	tenantId  string

	modelRegistry *ModelRegistry
}

func (r *ReadOnlyDataStore) ServiceCollection(name string) sdk.ReadOnlyCollection {
	collection := r.modelRegistry.Get(name)

	return &ReadOnlyCollection{
		client:     r.client,
		sessionId:  r.sessionId,
		tenantId:   r.tenantId,
		scope:      sdk.DataScopeService,
		name:       name,
		path:       name,
		parentPath: "",

		modelRegistry: r.modelRegistry,
		typeName:      collection.TypeName,
	}
}

func (r *ReadOnlyDataStore) AppCollection(name string) sdk.ReadOnlyCollection {
	collection := r.modelRegistry.Get(name)

	return &ReadOnlyCollection{
		client:     r.client,
		sessionId:  r.sessionId,
		tenantId:   r.tenantId,
		scope:      sdk.DataScopeApp,
		name:       name,
		path:       name,
		parentPath: "",

		modelRegistry: r.modelRegistry,
		typeName:      collection.TypeName,
	}
}

var _ sdk.ReadOnlyDataStore = (*ReadOnlyDataStore)(nil)

type DataStore struct {
	client    ServiceClient
	sessionId string
	tenantId  string

	modelRegistry *ModelRegistry
}

func (d *DataStore) ServiceCollection(name string) sdk.Collection {
	collection := d.modelRegistry.Get(name)

	return &Collection{
		client:     d.client,
		sessionId:  d.sessionId,
		tenantId:   d.tenantId,
		scope:      sdk.DataScopeService,
		path:       name,
		parentPath: "",

		modelRegistry: d.modelRegistry,
		typeName:      collection.TypeName,
	}
}

func (d *DataStore) AppCollection(name string) sdk.Collection {
	collection := d.modelRegistry.Get(name)

	return &Collection{
		client:     d.client,
		sessionId:  d.sessionId,
		tenantId:   d.tenantId,
		scope:      sdk.DataScopeApp,
		path:       name,
		parentPath: "",

		modelRegistry: d.modelRegistry,
		typeName:      collection.TypeName,
	}
}

var _ sdk.DataStore = (*DataStore)(nil)

type ReadOnlyCollection struct {
	client     ServiceClient
	sessionId  string
	tenantId   string
	scope      sdk.DataScope
	name       string
	path       string
	parentPath string

	modelRegistry *ModelRegistry
	typeName      string
}

func (c *ReadOnlyCollection) GetOne(id string) (sdk.ReadOnlyDoc, error) {
	data, err := c.client.GetData(c.sessionId, GetDataRequest{
		Scope:    c.scope,
		TenantId: c.tenantId,
		Path:     c.Path() + "/" + id,
	})

	if err != nil {
		return nil, err
	} else if !data.Exist {
		return nil, sdk.ErrNotFound
	}

	return &ReadOnlyDoc{
		client:    c.client,
		sessionId: c.sessionId,
		tenantId:  c.tenantId,
		scope:     c.scope,
		path:      c.Path() + "/" + id,
		version:   data.Version,
		item:      data.Data,

		modelRegistry: c.modelRegistry,
		typeName:      c.typeName,
	}, nil
}

func (c *ReadOnlyCollection) Query() sdk.ReadOnlyQuery {
	return &ReadOnlyQuery{
		client:         c.client,
		sessionId:      c.sessionId,
		tenantId:       c.tenantId,
		scope:          c.scope,
		collectionPath: c.Path(),

		modelRegistry: c.modelRegistry,
		typeName:      c.typeName,
	}
}

func (c *ReadOnlyCollection) Path() string {
	return c.path
}

var _ sdk.ReadOnlyCollection = (*ReadOnlyCollection)(nil)

type Collection struct {
	client     ServiceClient
	sessionId  string
	tenantId   string
	scope      sdk.DataScope
	name       string
	path       string
	parentPath string

	modelRegistry *ModelRegistry
	typeName      string
}

func (c *Collection) GetOne(id string) (sdk.Doc, error) {
	data, err := c.client.GetData(c.sessionId, GetDataRequest{
		Scope:    c.scope,
		TenantId: c.tenantId,
		Path:     c.Path() + "/" + id,
	})

	if err != nil {
		return nil, err
	} else if !data.Exist {
		return nil, sdk.ErrNotFound
	}

	return &Doc{
		client:    c.client,
		sessionId: c.sessionId,
		tenantId:  c.tenantId,
		scope:     c.scope,
		path:      c.Path() + "/" + id,
		version:   data.Version,
		item:      data.Data,

		modelRegistry: c.modelRegistry,
		typeName:      c.typeName,
	}, nil
}

func (c *Collection) InsertOne(id string, item interface{}, opts ...sdk.WriteOption) (sdk.Doc, error) {
	typeName := GetTypeName(item)

	if c.typeName == "" {
		fmt.Printf("inserting data into unregistered collection %s", c.Path())
	} else if typeName != c.typeName {
		return nil, fmt.Errorf("type name mismatch: expected %s, got %s", c.typeName, typeName)
	}

	cfg := &sdk.WriteConfig{
		VersionEquals: 0,
		ExpireIn:      0,
		Unsafe:        false,
		Upsert:        false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	var itemMap map[string]interface{}
	err := ConvertType(item, &itemMap)
	if err != nil {
		return nil, err
	}

	err = c.client.InsertData(c.sessionId, InsertDataRequest{
		Scope:          c.scope,
		TenantId:       c.tenantId,
		Path:           c.Path() + "/" + id,
		CollectionPath: c.Path(),
		ParentPath:     c.parentPath,
		Type:           c.name,
		Id:             id,
		Item:           itemMap,
		Cfg:            *cfg,
	})

	if err != nil {
		return nil, err
	}

	return &Doc{
		client:    c.client,
		sessionId: c.sessionId,
		tenantId:  c.tenantId,
		scope:     c.scope,
		path:      c.Path() + "/" + id,
		item:      itemMap,

		modelRegistry: c.modelRegistry,
		typeName:      c.typeName,
	}, nil
}

func (c *Collection) Query() sdk.Query {
	return &Query{
		client:         c.client,
		sessionId:      c.sessionId,
		tenantId:       c.tenantId,
		scope:          c.scope,
		collectionPath: c.Path(),

		modelRegistry: c.modelRegistry,
		typeName:      c.typeName,
	}
}

func (c *Collection) Path() string {
	return c.path
}

var _ sdk.Collection = (*Collection)(nil)

type ReadOnlyDoc struct {
	client    ServiceClient
	sessionId string
	tenantId  string
	scope     sdk.DataScope
	path      string
	version   int64
	item      map[string]interface{}

	modelRegistry *ModelRegistry
	typeName      string
}

func (r *ReadOnlyDoc) ChildCollection(name string) sdk.ReadOnlyCollection {
	collection := r.modelRegistry.Get(name)

	return &ReadOnlyCollection{
		client:     r.client,
		sessionId:  r.sessionId,
		tenantId:   r.tenantId,
		scope:      r.scope,
		name:       name,
		path:       r.Path() + "/" + name,
		parentPath: r.Path(),

		modelRegistry: r.modelRegistry,
		typeName:      collection.TypeName,
	}
}

func (r *ReadOnlyDoc) Path() string {
	return r.path
}

func (r *ReadOnlyDoc) Unmarshal(item interface{}) error {
	return ConvertType(r.item, item)
}

var _ sdk.ReadOnlyDoc = (*ReadOnlyDoc)(nil)

type Doc struct {
	client    ServiceClient
	sessionId string
	tenantId  string
	scope     sdk.DataScope
	path      string
	version   int64
	item      map[string]interface{}

	modelRegistry *ModelRegistry
	typeName      string
}

func (d *Doc) ExpireIn(expireIn time.Duration, opts ...sdk.WriteOption) error {
	cfg := &sdk.WriteConfig{
		VersionEquals: d.version,
		ExpireIn:      expireIn,
		Unsafe:        false,
		Upsert:        false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return d.client.UpdateTTL(d.sessionId, UpdateTTLRequest{
		Scope:    d.scope,
		TenantId: d.tenantId,
		Path:     d.Path(),
		Cfg:      *cfg,
	})
}

func (d *Doc) Update(item interface{}, opts ...sdk.WriteOption) error {
	typeName := GetTypeName(item)

	if d.typeName == "" {
		fmt.Printf("updating data into unregistered collection %s", d.Path())
	} else if d.typeName != typeName {
		return fmt.Errorf("type mismatch, expected: %s, given: %s", d.typeName, typeName)
	}

	cfg := &sdk.WriteConfig{
		VersionEquals: d.version,
		ExpireIn:      0,
		Unsafe:        false,
		Upsert:        false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	var itemMap map[string]interface{}
	err := ConvertType(item, &itemMap)
	if err != nil {
		return err
	}

	d.item = itemMap
	return d.client.UpdateData(d.sessionId, UpdateDataRequest{
		Scope:    d.scope,
		TenantId: d.tenantId,
		Path:     d.Path(),
		Item:     itemMap,
		Cfg:      *cfg,
	})
}

func (d *Doc) Delete(opts ...sdk.WriteOption) error {
	cfg := &sdk.WriteConfig{
		VersionEquals: d.version,
		ExpireIn:      0,
		Unsafe:        false,
		Upsert:        false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return d.client.DeleteData(d.sessionId, DeleteDataRequest{
		Scope:    d.scope,
		TenantId: d.tenantId,
		Path:     d.Path(),
		Cfg:      *cfg,
	})
}

func (d *Doc) ChildCollection(name string) sdk.Collection {
	collection := d.modelRegistry.Get(name)

	return &Collection{
		client:     d.client,
		sessionId:  d.sessionId,
		tenantId:   d.tenantId,
		scope:      d.scope,
		name:       name,
		path:       d.Path() + "/" + name,
		parentPath: d.Path(),

		modelRegistry: d.modelRegistry,
		typeName:      collection.TypeName,
	}
}

func (d *Doc) Path() string {
	return d.path
}

func (d *Doc) Unmarshal(item interface{}) error {
	return ConvertType(d.item, item)
}

var _ sdk.Doc = (*Doc)(nil)

type ReadOnlyQuery struct {
	client         ServiceClient
	sessionId      string
	tenantId       string
	scope          sdk.DataScope
	collectionPath string
	filter         string
	args           []any
	limit          int

	modelRegistry *ModelRegistry
	typeName      string
}

func (r *ReadOnlyQuery) Filter(expr string, args ...interface{}) sdk.ReadOnlyQuery {
	r.filter = expr
	r.args = args
	return r
}

func (r *ReadOnlyQuery) Limit(limit int) sdk.ReadOnlyQuery {
	r.limit = limit
	return r
}

func (r *ReadOnlyQuery) GetOne(ctx context.Context) (sdk.ReadOnlyDoc, error) {
	data, err := r.client.QueryData(r.sessionId, QueryDataRequest{
		Scope:          r.scope,
		TenantId:       r.tenantId,
		CollectionPath: r.collectionPath,
		Filter:         r.filter,
		Args:           r.args,
		Limit:          r.limit,
	})

	if err != nil {
		return nil, err
	} else if data.Data == nil || len(data.Data) == 0 {
		return nil, sdk.ErrNotFound
	}

	item := data.Data[0]
	return &ReadOnlyDoc{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     r.scope,
		path:      item.Path,
		version:   item.Version,
		item:      item.Data,

		modelRegistry: r.modelRegistry,
		typeName:      r.typeName,
	}, nil
}

func (r *ReadOnlyQuery) GetAll(ctx context.Context) ([]sdk.ReadOnlyDoc, error) {
	data, err := r.client.QueryData(r.sessionId, QueryDataRequest{
		Scope:          r.scope,
		TenantId:       r.tenantId,
		CollectionPath: r.collectionPath,
		Filter:         r.filter,
		Args:           r.args,
		Limit:          r.limit,
	})

	if err != nil {
		return nil, err
	}

	docs := make([]sdk.ReadOnlyDoc, 0)
	if data.Data != nil && len(data.Data) > 0 {
		for _, item := range data.Data {
			docs = append(docs, &ReadOnlyDoc{
				client:    r.client,
				sessionId: r.sessionId,
				tenantId:  r.tenantId,
				scope:     r.scope,
				path:      item.Path,
				version:   item.Version,
				item:      item.Data,

				modelRegistry: r.modelRegistry,
				typeName:      r.typeName,
			})
		}
	}

	return docs, nil
}

var _ sdk.ReadOnlyQuery = (*ReadOnlyQuery)(nil)

type Query struct {
	client         ServiceClient
	sessionId      string
	tenantId       string
	scope          sdk.DataScope
	collectionPath string
	filter         string
	args           []any
	limit          int

	modelRegistry *ModelRegistry
	typeName      string
}

func (q *Query) Filter(expr string, args ...interface{}) sdk.Query {
	q.filter = expr
	q.args = args
	return q
}

func (q *Query) Limit(limit int) sdk.Query {
	q.limit = limit
	return q
}

func (q *Query) GetOne(ctx context.Context) (sdk.Doc, error) {
	data, err := q.client.QueryData(q.sessionId, QueryDataRequest{
		Scope:          q.scope,
		TenantId:       q.tenantId,
		CollectionPath: q.collectionPath,
		Filter:         q.filter,
		Args:           q.args,
		Limit:          q.limit,
	})

	if err != nil {
		return nil, err
	} else if data.Data == nil || len(data.Data) == 0 {
		return nil, sdk.ErrNotFound
	}

	item := data.Data[0]
	return &Doc{
		client:    q.client,
		sessionId: q.sessionId,
		tenantId:  q.tenantId,
		scope:     q.scope,
		path:      item.Path,
		version:   item.Version,
		item:      item.Data,

		modelRegistry: q.modelRegistry,
		typeName:      q.typeName,
	}, nil
}

func (q *Query) GetAll(ctx context.Context) ([]sdk.Doc, error) {
	data, err := q.client.QueryData(q.sessionId, QueryDataRequest{
		Scope:          q.scope,
		TenantId:       q.tenantId,
		CollectionPath: q.collectionPath,
		Filter:         q.filter,
		Args:           q.args,
		Limit:          q.limit,
	})
	if err != nil {
		return nil, err
	}

	docs := make([]sdk.Doc, 0)
	if data.Data != nil && len(data.Data) > 0 {
		for _, item := range data.Data {
			docs = append(docs, &Doc{
				client:    q.client,
				sessionId: q.sessionId,
				tenantId:  q.tenantId,
				scope:     q.scope,
				path:      item.Path,
				version:   item.Version,
				item:      item.Data,

				modelRegistry: q.modelRegistry,
				typeName:      q.typeName,
			})
		}
	}

	return docs, nil
}

var _ sdk.Query = (*Query)(nil)
