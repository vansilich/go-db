package btree

import "encoding/binary"

// copy multiple KVs into the position from the old node
// n - не включительно
// TODO можно оптимизировать
func nodeAppendRange(new, old BNode, dstNew, srcOld, n uint16) error {
	idxDiff := dstNew - srcOld

	for i := uint16(0); i < n; i++ {
		idxOld := srcOld + i
		idxNew := idxOld + idxDiff

		// copy pointer
		oldPtr, err := old.getPtr(idxOld)
		if err != nil {
			return err
		}

		oldKey, err := old.getKey(idxOld)
		if err != nil {
			return err
		}

		oldVal, err := old.getVal(idxOld)
		if err != nil {
			return err
		}

		err = nodeAppendKV(new, idxNew, oldPtr, oldKey, oldVal)
		if err != nil {
			return err
		}
	}
	return nil
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key, val []byte) error {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos, err := new.KVpos(idx)
	if err != nil {
		return err
	}

	klen := uint16(len(key))
	binary.LittleEndian.PutUint16(new[pos+0:], klen)
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+klen:], val)

	// the offset of the next key
	offset, err := new.GetOffset(idx)
	if err != nil {
		return err
	}
	new.SetOffset(idx+1, offset+4+uint16((len(key)+len(val))))
	return nil
}
