// Package badgeropt holds the badger options shared by every badger DB the
// ledger opens (veritas/ledger and veritas/ledger/merkletree). It is a separate
// package so that both can import it without an import cycle.
package badgeropt

import "github.com/dgraph-io/badger/v3"

// Options returns badger.DefaultOptions(path) (same logger and other
// behaviour) with a low memory footprint. Each experiment node runs redis and
// a veritas server (veritas, veritastm, raftkv all use ledger.NewLedger) under a
// 768 MB container cap; the defaults (5 x 64 MB memtables plus a 256 MB block
// cache per DB) got nodes OOM-killed.
func Options(path string) badger.Options {
	opts := badger.DefaultOptions(path)
	opts.MemTableSize = 16 << 20
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 2
	opts.NumLevelZeroTablesStall = 4
	opts.BlockCacheSize = 32 << 20
	opts.IndexCacheSize = 16 << 20
	opts.ValueLogFileSize = 64 << 20
	return opts
}
