package btree

// add a new key to a leaf node
func leafInsert(new, old BNode, idx uint16, key, val []byte) error {
	new.SetHeader(BNODE_LEAF, old.nkeys()+1) // setup the header
	err := nodeAppendRange(new, old, 0, 0, idx)
	if err != nil {
		return err
	}

	err = nodeAppendKV(new, idx, 0, key, val)
	if err != nil {
		return err
	}

	return nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new, old BNode, idx uint16, key, val []byte) error {
	new.SetHeader(BNODE_LEAF, old.nkeys()) // setup the header
	err := nodeAppendRange(new, old, 0, 0, idx)
	if err != nil {
		return err
	}

	err = nodeAppendKV(new, idx, 0, key, val)
	if err != nil {
		return err
	}

	nextIdx := idx + 1
	return nodeAppendRange(new, old, nextIdx, nextIdx, old.nkeys()-nextIdx)
}

// remove a key from a leaf node
func leafDelete(new, old BNode, idx uint16) error {
	new.SetHeader(BNODE_LEAF, old.nkeys()-1) // setup the header
	err := nodeAppendRange(new, old, 0, 0, idx)
	if err != nil {
		return err
	}

	nextIdx := idx + 1
	return nodeAppendRange(new, old, idx, nextIdx, old.nkeys()-nextIdx)
}
