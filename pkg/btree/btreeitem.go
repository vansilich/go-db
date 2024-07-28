package btree

import (
	"encoding/binary"
	"errors"
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) SetHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) (uint64, error) {
	if idx > node.nkeys() {
		return 0, errors.New("idx is bigger then number of keys")
	}

	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:]), nil
}

func (node BNode) setPtr(idx uint16, val uint64) error {
	if idx > node.nkeys() {
		return errors.New("idx is bigger then number of keys")
	}
	idxStart := HEADER + 8*idx
	idxEnd := idxStart + 8

	binary.LittleEndian.PutUint64(node[idxStart:idxEnd], val)
	return nil
}

// offset list
func offsetPos(node BNode, idx uint16) (uint16, error) {
	if 1 > idx || idx > node.nkeys() {
		return 0, errors.New("offsetPos: wrong idx")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1), nil
}

func (node BNode) GetOffset(idx uint16) (uint16, error) {
	if idx == 0 {
		return 0, nil
	}

	pos, err := offsetPos(node, idx)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(node[pos:]), nil
}

func (node BNode) SetOffset(idx, offset uint16) error {
	pos, err := offsetPos(node, idx)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint16(node[pos:], offset)
	return nil
}

// key-values
func (node BNode) KVpos(idx uint16) (uint16, error) {
	if idx > node.nkeys() {
		return 0, errors.New("BNode.KVpos: idx is bigger than max key")
	}

	offset, err := node.GetOffset(idx)
	if err != nil {
		return 0, err
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + offset, nil
}

func (node BNode) kvBytes(idx uint16) (uint32, error) {
	if idx > node.nkeys() {
		return 0, errors.New("kvBytes: wrong idx")
	}

	pos, err := node.KVpos(idx)
	if err != nil {
		return 0, err
	}

	klen := binary.LittleEndian.Uint16(node[pos : pos+2])
	vlen := binary.LittleEndian.Uint16(node[pos+2 : pos+4])
	return uint32(4 + klen + vlen), nil
}

func (node BNode) getKey(idx uint16) ([]byte, error) {
	if idx > node.nkeys() {
		return []byte{}, errors.New("BNode.getKey: wrong idx")
	}

	pos, err := node.KVpos(idx)
	if err != nil {
		return []byte{}, err
	}

	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen], nil
}

func (node BNode) getVal(idx uint16) ([]byte, error) {
	if idx > node.nkeys() {
		return []byte{}, errors.New("getVal: wrong idx")
	}

	pos, err := node.KVpos(idx)
	if err != nil {
		return []byte{}, err
	}

	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen], nil
}

func (node BNode) NBytes() (uint16, error) {
	return node.KVpos(node.nkeys())
}
