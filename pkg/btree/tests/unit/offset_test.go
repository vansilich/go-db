package unit

import (
	"encoding/binary"
	"testing"

	"github.com/vansilich/db/pkg/btree"
)

func TestGetOffset(t *testing.T) {
	node := btree.BNode(make([]byte, btree.BTREE_PAGE_SIZE))
	node.SetHeader(btree.BNODE_LEAF, 1)

	offset, err := node.GetOffset(0)
	if err != nil {
		t.Fatalf("node.GetOffset: %s", err.Error())
	}

	if offset != uint16(0) {
		t.Fatalf("offset != 0")
	}
}

func TestSetOffset(t *testing.T) {
	idx := uint16(0)
	key := []byte("test_key")
	val := []byte("test_value")

	node := btree.BNode(make([]byte, btree.BTREE_PAGE_SIZE))
	node.SetHeader(btree.BNODE_LEAF, 1)

	pos, err := node.KVpos(0)
	if err != nil {
		t.Fatalf("node.KVpos: %s", err.Error())
	}

	klen := uint16(len(key))
	binary.LittleEndian.PutUint16(node[pos+0:], klen)
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))
	copy(node[pos+4:], key)
	copy(node[pos+4+klen:], val)

	// the offset of the next key
	offset, err := node.GetOffset(idx)
	if err != nil {
		t.Fatalf("node.GetOffset: %s", err.Error())
	}

	err = node.SetOffset(1, offset+4+uint16((len(key)+len(val))))
	if err != nil {
		t.Fatalf("node.SetOffset: %s", err.Error())
	}
}
