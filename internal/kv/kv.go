package kv

import (
	"syscall"

	"github.com/vansilich/db/pkg/btree"
	"github.com/vansilich/db/pkg/freelist"
)

// File structure:
//
// |     the_meta_page    | pages... | root_node | pages... | (end_of_file)
// | root_ptr | page_used |                ^                ^
// |          |                      |                |
// +----------|----------------------+                |
//			  |                                       |
//		   	  +---------------------------------------+

type KV struct {
	Path string // file name
	// internals
	fd   int
	tree btree.BTree
	free freelist.FreeList
	mmap struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
	failed bool // Did the last update fail?
}

func (db *KV) Open() error {
	db.tree.Get = db.pageRead   // read a page
	db.tree.New = db.pageAppend // apppend a page
	db.tree.Del = func(uint64) {}
	// free list callbacks
	db.free.Get = db.pageRead   // read a page
	db.free.New = db.pageAppend // append a page
	db.free.Set = db.pageWrite  // (new) in-place updates
	return nil
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key), true
}

func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db) // save the in-memory state (tree root)
	if err := db.tree.Insert(key, val); err != nil {
		return err
	}
	return updateOrRevert(db, meta)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted, err := db.tree.Delete(key)
	if err != nil {
		return false, err
	}
	return deleted, updateFile(db)
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 { // empty file
		db.page.flushed = 1 // the meta page is initialized on the 1st write
		return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		if err := updateRoot(db); err != nil {
			return err
		}

		if err := syscall.Fsync(db.fd); err != nil {
			return err
		}

		db.failed = false
	}

	err := updateFile(db)
	if err != nil {
		// the on-disk meta page is in an unknown state;
		// mark it to be rewritten on later recovery.
		db.failed = true
		// the in-memory states can be reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.temp = db.page.temp[:0]
	}

	return err
}
