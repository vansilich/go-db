package btree

import (
	"testing"
)

func TestSuccessfullyAppendKVToNewNode(t *testing.T) {
	var err error
	idx := uint16(0)

	old := BNode(make([]byte, BTREE_PAGE_SIZE))
	old.SetHeader(BNODE_LEAF, 4)
	if err = nodeAppendKV(old, idx, 0, []byte("key_1"), []byte("val_1")); err != nil {
		t.Fatal(err)
	}

	ptr, err := old.getPtr(idx)
	if err != nil {
		t.Fatal(err)
	}
	key, err := old.getKey(idx)
	if err != nil {
		t.Fatal(err)
	}
	val, err := old.getVal(idx)
	if err != nil {
		t.Fatal(err)
	}

	if ptr != uint64(0) {
		t.Fatal("unexpected ptr: %d", ptr)
	}
	if string(key) != "key_1" {
		t.Fatal("unexpected key: %s", key)
	}
	if string(val) != "val_1" {
		t.Fatal("unexpected value: %s", val)
	}
}
