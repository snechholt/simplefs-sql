package simplefs_sql

import (
	"reflect"
	"testing"
)

func TestMergeBufferItems(t *testing.T) {
	tests := []struct {
		name  string
		items bufferItemSlice
		want  bufferItemSlice
	}{
		{
			name: "Removes duplicate insert-dir items",
			items: bufferItemSlice{
				{Action: actionInsertDir, Name: "a/b/c"},
				{Action: actionInsertDir, Name: "d/e/f"},
				{Action: actionInsertDir, Name: "g/h/i"},
				{Action: actionInsertDir, Name: "a/b/c"},
				{Action: actionInsertDir, Name: "a/b/c"},
				{Action: actionInsertDir, Name: "d/e/f"},
			},
			want: bufferItemSlice{
				{Action: actionInsertDir, Name: "a/b/c"},
				{Action: actionInsertDir, Name: "d/e/f"},
				{Action: actionInsertDir, Name: "g/h/i"},
			},
		},
		{
			name: "Removes duplicate delete-file items",
			items: bufferItemSlice{
				{Action: actionDeleteFile, Name: "a/b/c"},
				{Action: actionDeleteFile, Name: "d/e/f"},
				{Action: actionDeleteFile, Name: "g/h/i"},
				{Action: actionDeleteFile, Name: "g/h/i"},
				{Action: actionDeleteFile, Name: "d/e/f"},
				{Action: actionDeleteFile, Name: "a/b/c"},
			},
			want: bufferItemSlice{
				{Action: actionDeleteFile, Name: "g/h/i"},
				{Action: actionDeleteFile, Name: "d/e/f"},
				{Action: actionDeleteFile, Name: "a/b/c"},
			},
		},
		{
			name: "Remove any write to a file before a delete",
			items: bufferItemSlice{
				{Action: actionInsertFile, Name: "a/b/c", FileContents: []byte{1}},
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{2}},
				{Action: actionInsertFile, Name: "g/h/i", FileContents: []byte{3}},
				{Action: actionDeleteFile, Name: "d/e/f"}, // <-- deletes d/e/f
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{4}},
			},
			want: bufferItemSlice{
				{Action: actionInsertFile, Name: "a/b/c", FileContents: []byte{1}},
				{Action: actionInsertFile, Name: "g/h/i", FileContents: []byte{3}},
				{Action: actionDeleteFile, Name: "d/e/f"}, // <-- deletes d/e/f
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{4}},
			},
		},
		{
			name: "Merge inserts to the same file",
			items: bufferItemSlice{
				{Action: actionInsertFile, Name: "a/b/c", FileContents: []byte{1}},
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{2}},
				{Action: actionInsertFile, Name: "g/h/i", FileContents: []byte{3}},
				{Action: actionInsertFile, Name: "a/b/c", FileContents: []byte{4}},
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{5}},
				{Action: actionInsertFile, Name: "a/b/c", FileContents: []byte{6}},
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{7}},
				{Action: actionInsertFile, Name: "g/h/i", FileContents: []byte{8}},
			},
			want: bufferItemSlice{
				{Action: actionInsertFile, Name: "a/b/c", FileContents: []byte{1, 4, 6}},
				{Action: actionInsertFile, Name: "d/e/f", FileContents: []byte{2, 5, 7}},
				{Action: actionInsertFile, Name: "g/h/i", FileContents: []byte{3, 8}},
			},
		},
		{
			name: "Combined test",
			items: bufferItemSlice{
				// File A is created. This is overwritten further down, but the insert-dir action
				// will be preserved, since it is the first entry.
				{Name: "A", Action: actionDeleteFile},
				{Name: "A", Action: actionInsertFile, FileContents: []byte{11}},
				{Name: "A", Action: actionInsertDir},

				// File B is created
				{Name: "B", Action: actionDeleteFile},
				{Name: "B", Action: actionInsertFile, FileContents: []byte{21}},
				{Name: "B", Action: actionInsertDir},

				// File A is appended to. This is overwritten further down
				{Name: "A", Action: actionInsertFile, FileContents: []byte{12}},
				{Name: "A", Action: actionInsertDir},

				// File A is overwritten
				{Name: "A", Action: actionDeleteFile},
				{Name: "A", Action: actionInsertFile, FileContents: []byte{111}},
				{Name: "A", Action: actionInsertDir},

				// File B is appended to
				{Name: "B", Action: actionInsertFile, FileContents: []byte{22}},
				{Name: "B", Action: actionInsertDir},

				// File A is overwritten again
				{Name: "A", Action: actionDeleteFile},
				{Name: "A", Action: actionInsertFile, FileContents: []byte{211}},
				{Name: "A", Action: actionInsertDir},

				// File A is appended to
				{Name: "A", Action: actionInsertFile, FileContents: []byte{212}},
				{Name: "A", Action: actionInsertDir},
			},
			want: bufferItemSlice{
				// The initial insert-dir action for file A is preserved
				{Name: "A", Action: actionInsertDir},

				// The payload for file B is merged
				{Name: "B", Action: actionDeleteFile},
				{Name: "B", Action: actionInsertFile, FileContents: []byte{21, 22}}, // <-- merged contents
				{Name: "B", Action: actionInsertDir},

				// File A is overwritten, insert-dit action is removed due to having an earlier entry
				{Name: "A", Action: actionDeleteFile},
				{Name: "A", Action: actionInsertFile, FileContents: []byte{211, 212}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := mergeBufferItems(test.items)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("Wrong items returned\nWant %v\nGot  %v", test.want, got)
			}
		})
	}
}
