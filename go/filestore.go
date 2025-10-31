package runtime

import (
	"encoding/base64"
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
	_, err := r.client.GetFile(r.sessionId, GetFileRequest{
		Scope:    r.scope,
		TenantId: r.tenantId,
		Path:     r.Path() + "/" + name + "/_folder.meta",
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
	res, err := r.client.GetFile(r.sessionId, GetFileRequest{
		Scope:    r.scope,
		TenantId: r.tenantId,
		Path:     r.Path() + "/" + name,
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
	res, err := r.client.ListFolder(r.sessionId, ListFolderRequest{
		Scope:       r.scope,
		TenantId:    r.tenantId,
		FolderPath:  r.path,
		Limit:       maxFiles,
		OffsetToken: offsetToken,
	})
	if err != nil {
		return nil, nil, err
	}

	var files []sdk.ReadOnlyFile
	for _, fileResp := range res.Files {
		files = append(files, &ReadOnlyFile{
			client:    r.client,
			sessionId: r.sessionId,
			tenantId:  r.tenantId,
			scope:     r.scope,
			path:      fileResp.Path,
			metadata:  fileResp.Metadata,
		})
	}

	return files, res.NextToken, nil
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
	_, err := f.client.GetFile(f.sessionId, GetFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.Path() + "/" + name + "/_folder.meta",
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
	fullPath := f.Path() + "/" + name
	err := f.client.CreateFolder(f.sessionId, CreateFolderRequest{
		Scope:      f.scope,
		TenantId:   f.tenantId,
		FolderPath: fullPath,
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
	res, err := f.client.GetFile(f.sessionId, GetFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.Path() + "/" + name,
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
	res, err := f.client.ListFolder(f.sessionId, ListFolderRequest{
		Scope:       f.scope,
		TenantId:    f.tenantId,
		FolderPath:  f.path,
		Limit:       maxFiles,
		OffsetToken: offsetToken,
	})
	if err != nil {
		return nil, nil, err
	}

	var files []sdk.File
	for _, fileResp := range res.Files {
		files = append(files, &File{
			client:    f.client,
			sessionId: f.sessionId,
			tenantId:  f.tenantId,
			scope:     f.scope,
			path:      fileResp.Path,
			metadata:  fileResp.Metadata,
		})
	}

	return files, res.NextToken, nil
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
	res, err := r.client.ReadFileContent(r.sessionId, ReadFileContentRequest{
		Scope:    r.scope,
		TenantId: r.tenantId,
		Path:     r.path,
	})
	if err != nil {
		return nil, err
	}
	// Decode base64 content from server
	decoded, err := base64.StdEncoding.DecodeString(res.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 content: %w", err)
	}
	return decoded, nil
}

func (r ReadOnlyFile) Download(localFilePath string) error {
	//TODO: implement local file download
	panic("implement me - requires file I/O")
}

func (r ReadOnlyFile) GetDownloadLink() (string, error) {
	res, err := r.client.GetFileDownloadLink(r.sessionId, GetFileRequest{
		Scope:    r.scope,
		TenantId: r.tenantId,
		Path:     r.path,
	})
	if err != nil {
		return "", err
	}
	return res.Link, nil
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
	res, err := f.client.ReadFileContent(f.sessionId, ReadFileContentRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.path,
	})
	if err != nil {
		return nil, err
	}
	// Decode base64 content from server
	decoded, err := base64.StdEncoding.DecodeString(res.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 content: %w", err)
	}
	return decoded, nil
}

func (f File) Download(filePath string) error {
	//TODO: implement local file download
	panic("implement me - requires file I/O")
}

func (f File) GetDownloadLink() (string, error) {
	res, err := f.client.GetFileDownloadLink(f.sessionId, GetFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.path,
	})
	if err != nil {
		return "", err
	}
	return res.Link, nil
}

func (f File) Save(data []byte) error {
	// Encode data to base64 for server
	encoded := base64.StdEncoding.EncodeToString(data)
	return f.client.PutFile(f.sessionId, PutFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.path,
		Content:  encoded,
	})
}

func (f File) Upload(filePath string) error {
	//TODO: implement local file upload
	panic("implement me - requires file I/O")
}

func (f File) GetUploadLink() (string, error) {
	res, err := f.client.GetFileUploadLink(f.sessionId, GetFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.path,
	})
	if err != nil {
		return "", err
	}
	return res.Link, nil
}

func (f File) Delete() error {
	return f.client.DeleteFile(f.sessionId, DeleteFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		Path:     f.path,
	})
}

func (f File) Rename(newName string) error {
	return f.client.RenameFile(f.sessionId, RenameFileRequest{
		Scope:    f.scope,
		TenantId: f.tenantId,
		OldPath:  f.path,
		NewPath:  newName,
	})
}

func (f File) MoveTo(dest sdk.Folder) error {
	// Read the current file content (returns decoded bytes)
	content, err := f.Read()
	if err != nil {
		return fmt.Errorf("failed to read file for move: %w", err)
	}

	// Get destination folder info
	destFolder, ok := dest.(*Folder)
	if !ok {
		return fmt.Errorf("destination is not a Folder type")
	}

	// Get just the filename from the current path
	fileName := f.metadata.Name
	destPath := destFolder.Path() + "/" + fileName

	// Encode content to base64 for server
	encoded := base64.StdEncoding.EncodeToString(content)

	// Write to destination
	err = f.client.PutFile(f.sessionId, PutFileRequest{
		Scope:    destFolder.scope,
		TenantId: destFolder.tenantId,
		Path:     destPath,
		Content:  encoded,
	})
	if err != nil {
		return fmt.Errorf("failed to write file to destination: %w", err)
	}

	// Delete original file
	err = f.Delete()
	if err != nil {
		return fmt.Errorf("failed to delete original file: %w", err)
	}

	return nil
}

func (f File) CopyTo(dest sdk.Folder) error {
	// Read the current file content (returns decoded bytes)
	content, err := f.Read()
	if err != nil {
		return fmt.Errorf("failed to read file for copy: %w", err)
	}

	// Get destination folder info
	destFolder, ok := dest.(*Folder)
	if !ok {
		return fmt.Errorf("destination is not a Folder type")
	}

	// Get just the filename from the current path
	fileName := f.metadata.Name
	destPath := destFolder.Path() + "/" + fileName

	// Encode content to base64 for server
	encoded := base64.StdEncoding.EncodeToString(content)

	// Write to destination
	err = f.client.PutFile(f.sessionId, PutFileRequest{
		Scope:    destFolder.scope,
		TenantId: destFolder.tenantId,
		Path:     destPath,
		Content:  encoded,
	})
	if err != nil {
		return fmt.Errorf("failed to write file to destination: %w", err)
	}

	return nil
}
