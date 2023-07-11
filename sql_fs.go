package simplefs_sql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/snechholt/simplefs"
	"io"
	"io/ioutil"
)

type DB interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(statement string, args ...interface{}) (sql.Result, error)
}

type FS struct {
	db        DB
	table     string
	container string

	buffer *buffer
}

func New(db DB, table, container string, bufferCapacity ...int) *FS {
	capacity := 0
	if len(bufferCapacity) > 0 {
		capacity = bufferCapacity[0]
	}
	buf := &buffer{db: db, table: table, container: container, capacity: capacity}
	return &FS{db: db, table: table, container: container, buffer: buf}
}

func (fs *FS) Open(name string) (simplefs.File, error) {
	if err := fs.Flush(); err != nil {
		return nil, err
	}
	rows, err := fs.db.Query("SELECT is_dir, contents FROM "+fs.table+" WHERE container = $1 AND path = $2 ORDER BY part",
		fs.container, name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var isDir bool
	var buf []byte
	var found bool
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&isDir, &b); err != nil {
			return nil, err
		}
		buf = append(buf, b...)
		found = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !found {
		return nil, simplefs.ErrNotFound
	}
	if isDir {
		return &sqlDir{fs: fs, name: name}, nil
	}
	return &sqlFile{name: name, r: ioutil.NopCloser(bytes.NewReader(buf))}, nil

	// The below code doesn't work with lib/pq but should work once we migrate over to a new library
	// Note that we need to remove the deferred rows.Close above for the below code to work
	// var closeInGoRoutine bool
	// defer func() {
	// 	if !closeInGoRoutine {
	// 		_ = rows.Close()
	// 	}
	// }()

	// if !rows.Next() {
	// 	if err := rows.Err(); err != nil {
	// 		return nil, err
	// 	}
	// 	return nil, simplefs.ErrNotFound
	// }
	//
	// var isDir bool
	// var b []byte
	// if err := rows.Scan(&isDir, &b); err != nil {
	// 	return nil, err
	// }
	//
	// if isDir {
	// 	return &sqlDir{fs: fs, name: name}, nil
	// }

	// pr, pw := io.Pipe()
	// f := &sqlFile{name: name, r: pr}
	//
	// closeInGoRoutine = true
	//
	// go func() {
	// 	defer func() { _ = rows.Close() }()
	//
	// 	_, _ = pw.Write(b)
	//
	// 	for rows.Next() {
	// 		var isDir bool
	// 		var b []byte
	// 		if err := rows.Scan(&isDir, &b); err != nil {
	// 			_ = pw.CloseWithError(err)
	// 			return
	// 		}
	// 		fmt.Println("\t", b)
	//
	// 		if _, err := pw.Write(b); err != nil {
	// 			return
	// 		}
	// 		fmt.Println("\tWrote bytes")
	// 	}
	// 	_ = pw.CloseWithError(rows.Err())
	// }()
	//
	// return f, nil
}

func (fs *FS) ReadDir(name string) ([]simplefs.DirEntry, error) {
	var entries []simplefs.DirEntry
	it := fs.readDirIterator(name)
	for {
		entry, err := it()
		if err != nil {
			return entries, err
		}
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		exists, err := fs.exists(name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, simplefs.ErrNotFound
		}
	}

	return entries, nil
}

func (fs *FS) exists(name string) (bool, error) {
	stmt := "SELECT 1 FROM " + fs.table + " WHERE container = $1 AND path = $2 LIMIT 1"
	rows, err := fs.db.Query(stmt, fs.container, name)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	exists := rows.Next()
	return exists, rows.Err()
}

func (fs *FS) readDirIterator(name string) func() (simplefs.DirEntry, error) {
	if err := fs.Flush(); err != nil {
		return func() (simplefs.DirEntry, error) { return nil, err }
	}
	stmt := "SELECT DISTINCT(name), is_dir FROM " + fs.table + " WHERE container = $1 AND dir = $2 ORDER BY name"
	rows, err := fs.db.Query(stmt, fs.container, name)
	if err != nil {
		return func() (simplefs.DirEntry, error) { return nil, err }
	}
	type Result struct {
		Entry dirEntry
		Err   error
	}
	ch := make(chan Result)
	go func() {
		defer func() { _ = rows.Close() }()
		defer close(ch)
		for rows.Next() {
			var entry dirEntry
			if err := rows.Scan(&entry.name, &entry.isDir); err != nil {
				ch <- Result{Err: err}
				return
			}
			ch <- Result{Entry: entry}
		}
		if err := rows.Err(); err != nil {
			ch <- Result{Err: err}
		}
	}()
	return func() (simplefs.DirEntry, error) {
		result, ok := <-ch
		if !ok {
			return nil, nil
		}
		return &result.Entry, result.Err
	}
}

func (fs *FS) Create(name string) (io.WriteCloser, error) {
	var buf bytes.Buffer
	return &writeCloser{
		w: &buf,
		closeFn: func() error {
			fs.buffer.Create(name, buf.Bytes())
			return fs.buffer.Flush(false)
		},
	}, nil
}

func (fs *FS) Append(name string) (io.WriteCloser, error) {
	var buf bytes.Buffer
	return &writeCloser{
		w: &buf,
		closeFn: func() error {
			fs.buffer.Append(name, buf.Bytes())
			return fs.buffer.Flush(false)
		},
	}, nil
}

func (fs *FS) Flush() error {
	return fs.buffer.Flush(true)
}

func (fs *FS) RemoveAllFiles() error {
	stmt := "DELETE FROM " + fs.table + " WHERE container = $1"
	_, err := fs.db.Exec(stmt, fs.container)
	return err
}

type sqlFile struct {
	name string
	r    io.ReadCloser
}

func (f *sqlFile) Read(p []byte) (n int, err error) {
	return f.r.Read(p)
}

func (f *sqlFile) Close() error {
	return f.r.Close()
}

func (f *sqlFile) ReadDir(n int) ([]simplefs.DirEntry, error) {
	return nil, fmt.Errorf("cannot ReadDir '%s'. Path is a file", f.name)
}

type sqlDir struct {
	fs   *FS
	name string

	readDirIterator func() (simplefs.DirEntry, error)
}

func (dir *sqlDir) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot read '%s'. Path is a directory", dir.name)
}

func (dir *sqlDir) Close() error {
	return nil
}

func (dir *sqlDir) ReadDir(n int) ([]simplefs.DirEntry, error) {
	if dir.readDirIterator == nil {
		dir.readDirIterator = dir.fs.readDirIterator(dir.name)
	}
	var entries []simplefs.DirEntry
	if n < 0 {
		for {
			entry, err := dir.readDirIterator()
			if err != nil {
				return entries, err
			}
			if entry == nil {
				break
			}
			entries = append(entries, entry)
		}
		return entries, nil
	}

	for i := 0; i < n; i++ {
		entry, err := dir.readDirIterator()
		if err != nil {
			return entries, err
		}
		if entry == nil {
			if i == 0 {
				return nil, io.EOF
			}
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

type dirEntry struct {
	name  string
	isDir bool
}

func (entry *dirEntry) Name() string {
	return entry.name
}

func (entry *dirEntry) IsDir() bool {
	return entry.isDir
}

func (entry *dirEntry) String() string {
	if entry == nil {
		return "<nil>"
	}
	if entry.isDir {
		return "dir(" + entry.name + ")"
	}
	return "file(" + entry.name + ")"
}
