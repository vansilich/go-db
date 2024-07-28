package freelist

import "github.com/vansilich/db/pkg/btree"

type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (btree.BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

type FreeList struct {
	// callbacks for managing on-disk pages
	Get func(uint64) []byte // read a page
	New func([]byte) uint64 // append a new page
	Set func(uint64) []byte // update an existing page
	// persisted data in the meta page
	headPage uint64 // pointer to the list head node
	headSeq  uint64 // monotonic sequence number to index into the list head
	tailPage uint64
	tailSeq  uint64
	// in-memory states
	maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
}

// getters & setters
func (node LNode) getNext() uint64
func (node LNode) setNext(next uint64)
func (node LNode) getPtr(idx int) uint64
func (node LNode) setPtr(idx int, ptr uint64)

// get 1 item from the list head. return 0 on failure.
func (fl *FreeList) PopHead() uint64

// add 1 item to the tail
func (fl *FreeList) PushTail(ptr uint64)

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

// make the newly added items available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

// remove 1 item from the head node, and remove the head node if empty.
func flPop(fl *FreeList) (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0 // cannot advance
	}
	node := LNode(fl.Get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq)) // item
	fl.headSeq++
	// move to the next one if the head node is empty
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		if fl.headPage != 0 {
			panic("flPop: fl.headPage != 0")
		}
	}
	return
}
