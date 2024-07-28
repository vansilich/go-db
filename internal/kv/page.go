package kv

import "github.com/vansilich/db/pkg/btree"

// `BTree.get`, read a page.
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/btree.BTREE_PAGE_SIZE
		if ptr < end {
			offset := btree.BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+btree.BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp)) // just append
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func (db *KV) pageWrite(ptr uint64) []byte {
	// TODO implement
	return []byte{}
}
