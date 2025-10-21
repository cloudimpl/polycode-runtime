package sdk

import "time"

type FileMetaData struct {
	Name     string    `json:"name"`
	Created  time.Time `json:"created"`
	Modified time.Time `json:"modified"`
	Size     int64     `json:"size"`
}

type ReadOnlyFileStoreBuilder interface {
	WithTenantId(tenantId string) ReadOnlyFileStoreBuilder
	Get() ReadOnlyFileStore
}

type ReadOnlyFileStore interface {
	ServiceFolder() ReadOnlyFolder
	AppFolder() ReadOnlyFolder
}

type FileStoreBuilder interface {
	WithTenantId(tenantId string) FileStoreBuilder
	Get() FileStore
}

type FileStore interface {
	ServiceFolder() Folder
	AppFolder() Folder
}

type ReadOnlyFile interface {
	Path() string
	Metadata() FileMetaData

	Read() ([]byte, error)
	Download(localFilePath string) error
	GetDownloadLink() (string, error)
}

type File interface {
	Path() string
	Metadata() FileMetaData

	Read() ([]byte, error)
	Download(filePath string) error
	GetDownloadLink() (string, error)

	Save(data []byte) error
	Upload(filePath string) error
	GetUploadLink() (string, error)

	Delete() error
	Rename(newName string) error
	MoveTo(dest Folder) error
	CopyTo(dest Folder) error
}

type ReadOnlyFolder interface {
	Path() string

	Folder(name string) (ReadOnlyFolder, error)
	File(name string) (ReadOnlyFile, error)
	List(maxFiles int32, offsetToken *string) ([]ReadOnlyFile, *string, error)
}

type Folder interface {
	Path() string

	Folder(name string) (Folder, error)
	CreateNewFolder(name string) (Folder, error)
	File(name string) (File, error)
	List(maxFiles int32, offsetToken *string) ([]File, *string, error)
}
