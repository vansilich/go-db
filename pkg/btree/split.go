package btree

import (
	"errors"
)

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode, error) {
	oldNbytes, err := old.NBytes()
	if err != nil {
		return 0, [3]BNode{}, err
	}

	if oldNbytes <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}, nil // no split
	}

	// split on 2 nodes
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // maybe split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	if err := nodeSplit2(left, right, old); err != nil {
		return 0, [3]BNode{}, err
	}

	leftNbytes, err := left.NBytes()
	if err != nil {
		return 0, [3]BNode{}, err
	}
	if leftNbytes <= BTREE_PAGE_SIZE {
		return 2, [3]BNode{left, right}, nil
	}

	// split on 3 nodes
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	if err := nodeSplit2(leftleft, middle, left); err != nil {
		return 0, [3]BNode{}, err
	}

	llNbytes, err := leftleft.NBytes()
	if err != nil {
		return 0, [3]BNode{}, err
	}
	if llNbytes > BTREE_PAGE_SIZE {
		return 0, [3]BNode{}, errors.New("llNbytes > BTREE_PAGE_SIZE")
	}

	return 3, [3]BNode{leftleft, middle, right}, nil // 3 nodes
}

// internal
// split a oversized node into 2 so that the 2nd node always fits on a page.
func nodeSplit2(left, right, old BNode) error {
	var err error

	nkeys := old.nkeys()

	currBytes := uint32(0)
	delimeter := uint16(0)
	for i := nkeys - 1; i != 0; i-- {
		kvlen, err := old.kvBytes(i)
		if err != nil {
			return err
		}

		if currBytes+kvlen > BTREE_PAGE_SIZE {
			delimeter = i + 1
			break
		}
		currBytes += kvlen
	}

	left.SetHeader(old.btype(), delimeter)
	if err = nodeAppendRange(left, old, 0, 0, delimeter); err != nil {
		return err
	}

	right.SetHeader(old.btype(), nkeys-delimeter)
	if err = nodeAppendRange(right, old, 0, delimeter, nkeys-delimeter); err != nil {
		return err
	}

	return nil
}
