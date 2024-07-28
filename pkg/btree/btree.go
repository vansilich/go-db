package btree

import (
	"bytes"
	"errors"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type BTree struct {
	// pointer (a nonzero page number)
	Root uint64
	// callbacks for managing on-disk pages
	Get func(uint64) []byte // dereference a pointer
	New func([]byte) uint64 // allocate a new page
	Del func(uint64)        // deallocate a page
}

// General node format:
// | type | nkeys |  pointers  |   offsets  | 		    key-values  		  | unused |
// |      |       | 		   | 		    | [ klen | vlen | key | val ] ... |        |
// |  2B  |   2B  | nkeys * 8B | nkeys * 2B | [  2B  |  2B  | ... | ... ] ... |		   |
//
// * type - node type (either BNODE_NODE or BNODE_LEAF)
// * nkeys - number of keys in this node
// * pointers - pointers to child nodes (used only when type=BNODE_NODE)
// * offsets - store start byte offset between every key-value pair. For example
// first key-value pair always have offset=0, second pair - `klen`+`vlen` of first pair and etc.
// * key-values - key-value pairs stored in this node.
// If type=BNODE_NODE only keys without values are stored
// * unused - unused space
type BNode []byte

const (
	BNODE_NODE                 = 1 // internal nodes without values
	BNODE_LEAF                 = 2 // leaf nodes with values
	SHOULD_MERGE_NO            = 0
	SHOULD_MERGE_LEFT_SIBLING  = -1
	SHOULD_MERGE_RIGHT_SIBLING = +1
)

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("node1max > BTREE_PAGE_SIZE") // maximum KV
	}
}

func (tree *BTree) Insert(key []byte, val []byte) error {
	if tree.Root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.SetHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		// (used for nodeLookupLE())
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.Root = tree.New(root)
		return nil
	}

	node, err := tree.treeInsert(tree.Get(tree.Root), key, val)
	if err != nil {
		return err
	}

	nsplit, splitedNodes, err := nodeSplit3(node)
	if err != nil {
		return err
	}

	tree.Del(tree.Root)

	if nsplit == 1 {
		tree.Root = tree.New(splitedNodes[0])
	} else {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.SetHeader(BNODE_NODE, nsplit)
		for i, knode := range splitedNodes[:nsplit] {
			ptr := tree.New(knode)
			if i == 0 {
				if err = nodeAppendKV(root, uint16(i), ptr, nil, nil); err != nil {
					return err
				}
			} else {
				key, err := knode.getKey(0)
				if err != nil {
					return err
				}
				if err = nodeAppendKV(root, uint16(i), ptr, key, nil); err != nil {
					return err
				}
			}
		}
		tree.Root = tree.New(root)
	}
	return nil
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func (tree *BTree) treeInsert(node BNode, key, val []byte) (BNode, error) {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	var new BNode = make([]byte, 2*BTREE_PAGE_SIZE)

	// where to insert the key?
	idx, err := nodeLookupLE(node, key)
	if err != nil {
		return new, err
	}

	// act depending on the node type
	switch node.btype() {
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		if err = nodeInsert(tree, new, node, idx, key, val); err != nil {
			return new, err
		}
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		idxkey, err := node.getKey(idx)
		if err != nil {
			return new, err
		}

		if bytes.Equal(key, idxkey) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			err = leafInsert(new, node, idx+1, key, val)
			if err != nil {
				return new, err
			}
		}
	default:
		return new, errors.New("bad node type")
	}

	return new, nil
}

// TODO implement
func (tree *BTree) Delete(key []byte) (bool, error) {
	if tree.Root == 0 {
		return false, errors.New("root node is not initialized")
	}

	node, err := tree.treeDelete(tree.Get(tree.Root), key)
	if err != nil {
		return false, err
	}

	return true, nil
}

// delete a key from the tree
// TODO разобраться с аллокацией/деалокацией
// TODO обрабатывать пустые новые ноды
func (tree *BTree) treeDelete(node BNode, key []byte) (BNode, error) {
	var err error = nil
	// the result node.
	var new BNode

	// where to insert the key?
	idx, err := nodeLookupLE(node, key)
	if err != nil {
		return new, err
	}

	// act depending on the node type
	switch node.btype() {
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		new, err = nodeDelete(tree, node, idx, key)
		if err != nil {
			return new, err
		}
	case BNODE_LEAF:
		// the result node.
		new = make([]byte, BTREE_PAGE_SIZE)
		if err = leafDelete(new, node, idx); err != nil {
			return new, err
		}
	default:
		return new, errors.New("bad node type")
	}

	return new, nil
}

// should the updated kid be merged with a sibling?
func (tree *BTree) shouldMerge(parent BNode, idx uint16, updated BNode) (int, BNode, error) {
	updNbytes, err := updated.NBytes()
	if err != nil {
		return SHOULD_MERGE_NO, BNode{}, err
	}

	if updNbytes > BTREE_PAGE_SIZE/4 {
		return SHOULD_MERGE_NO, BNode{}, nil
	}

	if idx > 0 {
		nptr, err := parent.getPtr(idx - 1)
		if err != nil {
			return SHOULD_MERGE_NO, BNode{}, err
		}

		sibling := BNode(tree.Get(nptr))
		sibNbytes, err := sibling.NBytes()
		if err != nil {
			return SHOULD_MERGE_NO, BNode{}, err
		}
		merged := sibNbytes + updNbytes - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return SHOULD_MERGE_LEFT_SIBLING, sibling, nil
		}
	}
	if idx+1 < parent.nkeys() {
		nptr, err := parent.getPtr(idx + 1)
		if err != nil {
			return SHOULD_MERGE_NO, BNode{}, err
		}

		sibling := BNode(tree.Get(nptr))
		sibNbytes, err := sibling.NBytes()
		if err != nil {
			return SHOULD_MERGE_NO, BNode{}, err
		}
		merged := sibNbytes + updNbytes - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return SHOULD_MERGE_RIGHT_SIBLING, sibling, nil
		}
	}
	return SHOULD_MERGE_NO, BNode{}, nil
}
