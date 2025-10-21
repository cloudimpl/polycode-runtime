package runtime

import (
	"fmt"
	"github.com/cloudimpl/polycode-runtime/go/sdk"
)

type ReadOnlyFileStoreBuilder struct {
	client    ServiceClient
	sessionId string
	tenantId  string
}

func (r *ReadOnlyFileStoreBuilder) WithTenantId(tenantId string) sdk.ReadOnlyFileStoreBuilder {
	r.tenantId = tenantId
	return r
}

func (r *ReadOnlyFileStoreBuilder) Get() sdk.ReadOnlyFileStore {
	return &ReadOnlyFileStore{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
	}
}

type ReadOnlyFileStore struct {
	client    ServiceClient
	sessionId string
	tenantId  string
}

func (r *ReadOnlyFileStore) ServiceFolder() sdk.ReadOnlyFolder {
	return &ReadOnlyFolder{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     sdk.DataScopeService,
		path:      "",
	}
}

func (r *ReadOnlyFileStore) AppFolder() sdk.ReadOnlyFolder {
	return &ReadOnlyFolder{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     sdk.DataScopeApp,
		path:      "",
	}
}

type FileStoreBuilder struct {
	client    ServiceClient
	sessionId string
	tenantId  string
}

func (r *FileStoreBuilder) WithTenantId(tenantId string) sdk.FileStoreBuilder {
	r.tenantId = tenantId
	return r
}

func (r *FileStoreBuilder) Get() sdk.FileStore {
	return &FileStore{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
	}
}

type FileStore struct {
	client    ServiceClient
	sessionId string
	tenantId  string
}

func (r *FileStore) ServiceFolder() sdk.Folder {
	return &Folder{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     sdk.DataScopeService,
		path:      "",
	}
}

func (r *FileStore) AppFolder() sdk.Folder {
	return &Folder{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     sdk.DataScopeApp,
		path:      "",
	}
}

type ReadOnlyFolder struct {
	client    ServiceClient
	sessionId string
	tenantId  string
	scope     sdk.DataScope
	path      string
}

func (r *ReadOnlyFolder) Path() string {
	return r.path
}

func (r *ReadOnlyFolder) Folder(name string) (sdk.ReadOnlyFolder, error) {
	_, err := r.client.GetFile(r.sessionId, GetScopeFileRequest{
		Scope: r.scope,
		Request: GetFileRequest{
			TenantId: r.tenantId,
			Path:     r.Path() + "/" + name + "/_file.meta",
		},
	})
	if err != nil {
		return nil, err
	}

	return &ReadOnlyFolder{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     r.scope,
		path:      r.Path() + "/" + name,
	}, nil
}

func (r *ReadOnlyFolder) File(name string) (sdk.ReadOnlyFile, error) {
	res, err := r.client.GetFile(r.sessionId, GetScopeFileRequest{
		Scope: r.scope,
		Request: GetFileRequest{
			TenantId: r.tenantId,
			Path:     r.Path() + "/" + name,
		},
	})
	if err != nil {
		return nil, err
	}

	return &ReadOnlyFile{
		client:    r.client,
		sessionId: r.sessionId,
		tenantId:  r.tenantId,
		scope:     r.scope,
		path:      r.Path() + "/" + name,
		metadata:  res.Metadata,
	}, nil
}

func (r *ReadOnlyFolder) List(maxFiles int32, offsetToken *string) ([]sdk.ReadOnlyFile, *string, error) {
	//TODO implement me
	panic("implement me")
}

type Folder struct {
	client    ServiceClient
	sessionId string
	tenantId  string
	scope     sdk.DataScope
	path      string
}

func (f *Folder) Path() string {
	return f.path
}

func (f *Folder) Folder(name string) (sdk.Folder, error) {
	_, err := f.client.GetFile(f.sessionId, GetScopeFileRequest{
		Scope: f.scope,
		Request: GetFileRequest{
			TenantId: f.tenantId,
			Path:     f.Path() + "/" + name + "/_file.meta",
		},
	})
	if err != nil {
		return nil, err
	}

	return &Folder{
		client:    f.client,
		sessionId: f.sessionId,
		tenantId:  f.tenantId,
		scope:     f.scope,
		path:      f.Path() + "/" + name,
	}, nil
}

func (f *Folder) CreateNewFolder(name string) (sdk.Folder, error) {
	err := f.client.CreateFolder(f.sessionId, CreateScopeFolderRequest{
		Scope: f.scope,
		Request: CreateFolderRequest{
			FolderPath: name,
		},
	})
	if err != nil {
		fmt.Printf("failed to create folder: %s\n", err.Error())
		return nil, err
	}

	return &Folder{
		client:    f.client,
		sessionId: f.sessionId,
		tenantId:  f.tenantId,
		scope:     f.scope,
		path:      f.Path() + "/" + name,
	}, nil
}

func (f *Folder) File(name string) (sdk.File, error) {
	res, err := f.client.GetFile(f.sessionId, GetScopeFileRequest{
		Scope: f.scope,
		Request: GetFileRequest{
			TenantId: f.tenantId,
			Path:     f.Path() + "/" + name,
		},
	})
	if err != nil {
		return nil, err
	}

	return &File{
		client:    f.client,
		sessionId: f.sessionId,
		tenantId:  f.tenantId,
		scope:     f.scope,
		path:      f.Path() + "/" + name,
		metadata:  res.Metadata,
	}, nil
}

func (f *Folder) List(maxFiles int32, offsetToken *string) ([]sdk.File, *string, error) {
	//TODO implement me
	panic("implement me")
}

type ReadOnlyFile struct {
	client    ServiceClient
	sessionId string
	tenantId  string
	scope     sdk.DataScope
	path      string
	metadata  sdk.FileMetaData
}

func (r ReadOnlyFile) Path() string {
	return r.path
}

func (r ReadOnlyFile) Metadata() sdk.FileMetaData {
	return r.metadata
}

func (r ReadOnlyFile) Read() ([]byte, error) {
	panic("implement me")
}

func (r ReadOnlyFile) Download(localFilePath string) error {
	panic("implement me")
}

func (r ReadOnlyFile) GetDownloadLink() (string, error) {
	panic("implement me")
}

type File struct {
	client    ServiceClient
	sessionId string
	tenantId  string
	scope     sdk.DataScope
	path      string
	metadata  sdk.FileMetaData
}

func (f File) Path() string {
	return f.path
}

func (f File) Metadata() sdk.FileMetaData {
	return f.metadata
}

func (f File) Read() ([]byte, error) {
	panic("implement me")
}

func (f File) Download(filePath string) error {
	panic("implement me")
}

func (f File) GetDownloadLink() (string, error) {
	panic("implement me")
}

func (f File) Save(data []byte) error {
	panic("implement me")
}

func (f File) Upload(filePath string) error {
	panic("implement me")
}

func (f File) GetUploadLink() (string, error) {
	panic("implement me")
}

func (f File) Delete() error {
	panic("implement me")
}

func (f File) Rename(newName string) error {
	panic("implement me")
}

func (f File) MoveTo(dest sdk.Folder) error {
	panic("implement me")
}

func (f File) CopyTo(dest sdk.Folder) error {
	panic("implement me")
}
