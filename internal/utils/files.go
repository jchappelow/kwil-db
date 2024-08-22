package utils

import (
	"crypto/sha256"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func NewDirFS(dir string) DirFS {
	return DirFS{
		FS:   os.DirFS(dir),
		path: dir,
	}
}

type DirFS struct {
	fs.FS // os.DirFS
	path  string
}

func (df DirFS) WriteFile(name string, data []byte) error {
	fp := filepath.Join(df.path, name)
	return os.WriteFile(fp, data, 0755)
}

func ReadOrCreateFile(path string) ([]byte, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func CreateOrOpenFile(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func HashFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := sha256.New()

	if _, err := io.Copy(hash, file); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
