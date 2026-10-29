package badgeropt

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
)

func TestOptionsAreLowMemoryAndOpen(t *testing.T) {
	dir := t.TempDir()
	o := Options(dir)
	if o.Dir != dir || o.ValueDir != dir {
		t.Fatalf("Dir=%q ValueDir=%q want %q", o.Dir, o.ValueDir, dir)
	}
	if o.MemTableSize != 16<<20 || o.NumMemtables != 2 || o.NumLevelZeroTables != 2 ||
		o.NumLevelZeroTablesStall != 4 || o.BlockCacheSize != 32<<20 ||
		o.IndexCacheSize != 16<<20 || o.ValueLogFileSize != 64<<20 {
		t.Fatalf("unexpected options: %+v", o)
	}
	db, err := badger.Open(o)
	if err != nil {
		t.Fatalf("open with low-memory options: %v", err)
	}
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set([]byte("k"), make([]byte, 1000)) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
