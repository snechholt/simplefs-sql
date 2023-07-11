package simplefs_sql

import (
	"database/sql"
	"github.com/snechholt/simplefs"
	"testing"

	_ "github.com/lib/pq" // postgres
)

func TestFS(t *testing.T) {
	const connStr = "host=127.0.0.1 port=5432 user=postgres password=password dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	t.Run("Without buffering", func(t *testing.T) {
		trx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = trx.Rollback() }()

		fs := New(trx, "file", "container1")
		if msg := simplefs.RunFileSystemTest(fs); msg != "" {
			t.Fatal(msg)
		}
	})

	t.Run("With buffering", func(t *testing.T) {
		trx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = trx.Rollback() }()

		capacity := 5 << (10 * 2)
		fs := New(trx, "file", "container1", capacity)
		if msg := simplefs.RunFileSystemTest(fs); msg != "" {
			t.Fatal(msg)
		}
	})

	// TODO: test RemoveAllFiles
	// TODO: test that containers are isolated (save same file in two fs)
}
