package btree

import (
	"bytes"
	"errors"
)

// returns the first kid node whose range intersects the key (kid[i] <= key).
// (LE - Less-than-or-Equal)
func nodeLookupLE(node BNode, key []byte) (uint16, error) {
	nkeys := node.nkeys()
	found := uint16(0)

	var nodeBinSearch func(startIdx, endIdx uint16) error
	nodeBinSearch = func(startIdx, endIdx uint16) error {
		if startIdx >= endIdx {
			return nil
		}

		idx := uint16(startIdx + ((endIdx - startIdx) / 2))
		nodekey, err := node.getKey(idx)
		if err != nil {
			return err
		}

		// if key less or equal
		if cmp := bytes.Compare(nodekey, key); cmp <= 0 {
			found = idx
			return nodeBinSearch(idx+1, endIdx)
		} else {
			return nodeBinSearch(startIdx, idx-1)
		}
	}

	// the key by idx=0 is always 'nil' in internal node,
	// thus it's always less than or equal to the key.
	if err := nodeBinSearch(1, nkeys-1); err != nil {
		return 0, err
	}

	return found, nil
}

// KV insertion to an internal node; part of the treeInsert()
func nodeInsert(tree *BTree, new, old BNode, idx uint16, key, val []byte) error {
	kidPtr, err := old.getPtr(idx)
	if err != nil {
		return err
	}

	// recursive insertion to the kid node
	kidNode, err := tree.treeInsert(tree.Get(kidPtr), key, val)
	if err != nil {
		return err
	}

	// split the result
	nsplit, split, err := nodeSplit3(kidNode)
	if err != nil {
		return err
	}

	// deallocate the kid node
	tree.Del(kidPtr)

	// update the kid links
	return nodeReplaceKidNode(tree, new, old, idx, split[:nsplit]...)
}

// replace a link with one or multiple links
func nodeReplaceKidNode(tree *BTree, new, old BNode, idx uint16, kids ...BNode) error {
	inc := uint16(len(kids))
	new.SetHeader(BNODE_NODE, old.nkeys()+inc-1)

	if err := nodeAppendRange(new, old, 0, 0, idx); err != nil {
		return err
	}

	for i, node := range kids {
		if i == 0 {
			nodeAppendKV(new, idx+uint16(i), tree.New(node), nil, nil)
		} else {
			key, err := node.getKey(0)
			if err != nil {
				return err
			}
			if err = nodeAppendKV(new, idx+uint16(i), tree.New(node), key, nil); err != nil {
				return err
			}
		}
	}

	return nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// delete a key from an internal node; part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) (BNode, error) {
	// recurse into the kid
	kidptr, err := node.getPtr(idx)
	if err != nil {
		return BNode{}, err
	}

	updated, err := tree.treeDelete(tree.Get(kidptr), key)
	if err != nil {
		return BNode{}, err
	}

	if len(updated) == 0 {
		return BNode{}, nil // not found
	}

	tree.Del(kidptr)

	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling, err := tree.shouldMerge(node, idx, updated)
	if err != nil {
		return BNode{}, err
	}

	switch {
	case mergeDir == SHOULD_MERGE_LEFT_SIBLING:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))

		err = nodeMerge(merged, sibling, updated)
		if err != nil {
			return BNode{}, err
		}

		nptr, err := node.getPtr(idx - 1)
		if err != nil {
			return BNode{}, err
		}

		tree.Del(nptr)

		nkey, err := merged.getKey(0)
		if err != nil {
			return BNode{}, err
		}

		if err = nodeReplace2Kid(newNode, node, idx-1, tree.New(merged), nkey); err != nil {
			return BNode{}, err
		}
	case mergeDir == SHOULD_MERGE_RIGHT_SIBLING:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))

		err = nodeMerge(merged, updated, sibling)
		if err != nil {
			return BNode{}, err
		}

		nptr, err := node.getPtr(idx + 1)
		if err != nil {
			return BNode{}, err
		}

		tree.Del(nptr)

		nkey, err := merged.getKey(0)
		if err != nil {
			return BNode{}, err
		}

		if err = nodeReplace2Kid(newNode, node, idx, tree.New(merged), nkey); err != nil {
			return BNode{}, err
		}
	case mergeDir == SHOULD_MERGE_NO && updated.nkeys() == 0:
		if !(node.nkeys() == 1 && idx == 0) {
			return BNode{}, errors.New("must be 1 empty child and no sibling")
		}
		newNode.SetHeader(BNODE_NODE, 0) // the parent becomes empty too
	case mergeDir == SHOULD_MERGE_NO && updated.nkeys() > 0: // no merge
		err = nodeReplaceKidNode(tree, newNode, node, idx, updated)
		if err != nil {
			return BNode{}, err
		}
	}
	return newNode, nil
}

// merge 2 nodes into 1
func nodeMerge(new, left, right BNode) error {
	var err error = nil

	leftNKeys, rightNKeys := left.nkeys(), right.nkeys()
	new.SetHeader(left.btype(), leftNKeys+rightNKeys)

	leftIdx, rightIdx := uint16(0), uint16(0)

	var iterations uint32 = 0
	if leftNKeys > 0 {
		iterations += uint32(leftNKeys - 1)
	}
	if rightNKeys > 0 {
		iterations += uint32(rightNKeys - 1)
	}

	for i := uint32(0); i <= iterations; i++ {
		leftKey, rightKey := []byte{}, []byte{}

		if leftIdx < leftNKeys {
			if leftKey, err = left.getKey(leftIdx); err != nil {
				return err
			}
		}
		if rightIdx < rightNKeys {
			if rightKey, err = right.getKey(rightIdx); err != nil {
				return err
			}
		}

		cmp := bytes.Compare(leftKey, rightKey)
		if cmp == 0 {
			return errors.New("left and right nodes cant have equal keys")
		}

		if cmp < 0 && leftIdx != leftNKeys {
			// leftKey < rightKey
			ptr, err := left.getPtr(leftIdx)
			if err != nil {
				return err
			}

			val, err := left.getVal(leftIdx)
			if err != nil {
				return err
			}

			if err = nodeAppendKV(new, leftIdx, ptr, leftKey, val); err != nil {
				return err
			}
			leftIdx++
		} else {
			// leftKey > rightKey
			ptr, err := right.getPtr(rightIdx)
			if err != nil {
				return err
			}

			val, err := right.getVal(rightIdx)
			if err != nil {
				return err
			}

			if err = nodeAppendKV(new, rightIdx, ptr, rightKey, val); err != nil {
				return err
			}
			rightIdx++
		}
	}

	return nil
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(new, old BNode, fromIdx uint16, ptr uint64, key []byte) error {
	new.SetHeader(BNODE_NODE, old.nkeys()-1)
	var err error

	if err = nodeAppendRange(new, old, 0, 0, fromIdx); err != nil {
		return err
	}

	if err = nodeAppendKV(new, fromIdx, ptr, key, nil); err != nil {
		return err
	}

	return nodeAppendRange(new, old, fromIdx+1, fromIdx+2, old.nkeys()-(fromIdx+2))
}
