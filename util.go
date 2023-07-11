package simplefs_sql

import (
	"io"
	"path"
)

type writeCloser struct {
	w       io.Writer
	closeFn func() error
}

func (w *writeCloser) Write(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func (w *writeCloser) Close() error {
	if w.closeFn != nil {
		return w.closeFn()
	}
	return nil
}

func splitPath(name string) (dir, file string) {
	dir, file = path.Split(name)
	if n := len(dir); n > 0 {
		dir = dir[:n-1]
	}
	return dir, file
}
