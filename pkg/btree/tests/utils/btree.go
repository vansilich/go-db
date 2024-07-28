package utils

import (
	"unsafe"

	"github.com/vansilich/db/pkg/btree"
)

type C struct {
	Tree  btree.BTree
	Ref   map[string]string      // the reference data
	Pages map[uint64]btree.BNode // in-memory pages
}

func NewC() *C {
	pages := map[uint64]btree.BNode{}
	return &C{
		Tree: btree.BTree{
			Get: func(ptr uint64) []byte {
				page, ok := pages[ptr]
				if !ok {
					panic("BTree.Get: undefined page ptr")
				}
				return page
			},
			New: func(node []byte) uint64 {
				nbytes, err := btree.BNode(node).NBytes()
				if err != nil {
					panic(err)
				}
				if nbytes > btree.BTREE_PAGE_SIZE {
					panic("BTree.New: nbytes > btree.BTREE_PAGE_SIZE")
				}
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				if !(pages[ptr] == nil) {
					panic("BTree.New: unknown ptr")
				}
				pages[ptr] = node
				return ptr
			},
			Del: func(ptr uint64) {
				if pages[ptr] == nil {
					panic("BTree.Del: unknown ptr")
				}
				delete(pages, ptr)
			},
		},
		Ref:   map[string]string{},
		Pages: pages,
	}
}

func (c *C) Add(key string, val string) error {
	err := c.Tree.Insert([]byte(key), []byte(val))
	if err != nil {
		return err
	}
	c.Ref[key] = val // reference data
	return nil
}
