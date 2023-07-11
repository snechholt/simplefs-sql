package simplefs_sql

import (
	"fmt"
	"strings"
)

type buffer struct {
	db        DB
	table     string
	container string
	items     bufferItemSlice
	capacity  int
}

type action uint8

const (
	actionDeleteFile action = iota + 1
	actionInsertFile
	actionInsertDir
)

func (a action) String() string {
	switch a {
	case actionDeleteFile:
		return "delete"
	case actionInsertFile:
		return "insert-file"
	case actionInsertDir:
		return "insert-dir"
	default:
		return "<invalid action>"
	}
}

type bufferItem struct {
	Name         string
	Action       action
	FileContents []byte
}

type bufferItemSlice []bufferItem

func (s bufferItemSlice) Filter(fn func(item bufferItem) bool) bufferItemSlice {
	var result bufferItemSlice
	for _, item := range s {
		if fn(item) {
			result = append(result, item)
		}
	}
	return result
}

func (s bufferItemSlice) WithAction(a action) bufferItemSlice {
	return s.Filter(func(item bufferItem) bool { return item.Action == a })
}

func (item bufferItem) String() string {
	if item.FileContents == nil {
		return fmt.Sprintf("{ Name:'%s', Action:'%s' }", item.Name, item.Action)
	}
	return fmt.Sprintf("{ Name:'%s', Action:'%s', B:%v }", item.Name, item.Action, item.FileContents)
}

func mergeBufferItems(items bufferItemSlice) bufferItemSlice {
	skip := make([]bool, len(items))
	var skipped int
	n := len(items)

	// Remove duplicate insert-dir actions
	{
		seen := make(map[string]bool)
		for i, item := range items {
			if item.Action == actionInsertDir {
				if _, ok := seen[item.Name]; ok {
					skip[i] = true
				} else {
					seen[item.Name] = true
				}
			}
		}
	}

	// Remove duplicate delete-file actions
	{
		seen := make(map[string]bool)
		for i := n - 1; i >= 0; i-- {
			item := items[i]
			if item.Action == actionDeleteFile {
				if _, ok := seen[item.Name]; ok {
					skip[i] = true
				} else {
					seen[item.Name] = true
				}
			}
		}
	}

	// Remove inserts before deletes
	for i := n - 1; i >= 0; i-- {
		if delItem := items[i]; delItem.Action == actionDeleteFile {
			for j := 0; j < i; j++ {
				insItem := items[j]
				if insItem.Action == actionInsertFile && insItem.Name == delItem.Name {
					skip[j] = true
				}
			}
		}
	}

	// Merge inserts for the same file
	for i, item := range items {
		if item.Action == actionInsertFile && !skip[i] {
			for j := i + 1; j < n; j++ {
				dupeItem := items[j]
				if !skip[j] && dupeItem.Action == actionInsertFile && dupeItem.Name == item.Name {
					items[i].FileContents = append(items[i].FileContents, dupeItem.FileContents...)
					skip[j] = true
				}
			}
		}
	}

	cpy := make(bufferItemSlice, 0, n-skipped)
	for i, item := range items {
		if !skip[i] {
			cpy = append(cpy, item)
		}
	}

	return cpy
}

func (buf *buffer) Create(name string, b []byte) {
	buf.items = append(buf.items,
		bufferItem{Name: name, Action: actionDeleteFile},
		bufferItem{Name: name, Action: actionInsertFile, FileContents: b},
		bufferItem{Name: name, Action: actionInsertDir},
	)
}

func (buf *buffer) Append(name string, b []byte) {
	buf.items = append(buf.items,
		bufferItem{Name: name, Action: actionInsertFile, FileContents: b},
		bufferItem{Name: name, Action: actionInsertDir},
	)
}

func (buf *buffer) size() int {
	var size int
	for _, item := range buf.items {
		size += len(item.Name) + len(item.FileContents)
	}
	return size
}

func (buf *buffer) Flush(force bool) error {
	if doFlush := force || buf.capacity <= 0 || buf.size() > buf.capacity; !doFlush {
		return nil
	}

	buf.items = mergeBufferItems(buf.items)

	if doFlush := force || buf.capacity <= 0 || buf.size() > buf.capacity; !doFlush {
		return nil
	}

	// Deletes
	if deletes := buf.items.WithAction(actionDeleteFile); len(deletes) > 0 {
		values := make([]interface{}, 1+len(deletes))
		insSlice := make([]string, len(deletes))
		values[0] = buf.container
		for i, item := range deletes {
			insSlice[i] = fmt.Sprintf("$%d", i+2)
			values[i+1] = item.Name
		}
		ins := strings.Join(insSlice, ",")
		stmt := "DELETE FROM " + buf.table + " WHERE container = $1 AND name IN (" + ins + ")"
		if _, err := buf.db.Exec(stmt, values...); err != nil {
			return err
		}
	}

	// Insert dirs
	if dirs := buf.items.WithAction(actionInsertDir); len(dirs) > 0 {
		var paramsSlice []string
		values := []interface{}{buf.container}
		for _, item := range dirs {
			split := strings.Split(item.Name, "/")
			n := len(split)
			for i := 0; i < n-1; i++ {
				p := len(values)
				valueStmt := fmt.Sprintf("($1, $%d, 0, $%d, $%d, true)", p+1, p+2, p+3)
				paramsSlice = append(paramsSlice, valueStmt)

				dirPath := strings.Join(split[:i+1], "/")
				dir := strings.Join(split[:i], "/")
				file := split[i]
				values = append(values, dirPath, dir, file)
			}
		}
		if len(paramsSlice) > 0 {
			stmt := `
				INSERT INTO ` + buf.table + `
				(container, path, part, dir, name, is_dir)
				VALUES ` + strings.Join(paramsSlice, ",") + `
				ON CONFLICT (container, path, part) DO NOTHING`
			if _, err := buf.db.Exec(stmt, values...); err != nil {
				return fmt.Errorf("error inserting dir rows: %v", err)
			}
		}
	}

	// Insert files
	if files := buf.items.WithAction(actionInsertFile); len(files) > 0 {
		var paramsSlice []string
		values := []interface{}{buf.container}
		for _, item := range files {
			p := len(values)
			valueStmt := fmt.Sprintf("($1, $%d, $%d, $%d, false, $%d)", p+1, p+2, p+3, p+4)
			paramsSlice = append(paramsSlice, valueStmt)
			dir, file := splitPath(item.Name)
			values = append(values, item.Name, dir, file, item.FileContents)
		}
		stmt := `
			INSERT INTO ` + buf.table + `
			(container, path, dir, name, is_dir, contents)
			VALUES ` + strings.Join(paramsSlice, ",")
		if _, err := buf.db.Exec(stmt, values...); err != nil {
			return fmt.Errorf("error inserting file rows: %v", err)
		}
	}

	buf.items = nil

	return nil
}
