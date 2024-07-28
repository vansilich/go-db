package kv

import "encoding/binary"

// Structure of meta header :
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |

const DB_SIG = "BuildYourOwnDB06" // not compatible between chapters
const META_SIZE_IN_BYTES = 32

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.Root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	sig := string(data[:16])
	if sig != DB_SIG {
		panic("loadMeta: bad sig")
	}

	db.tree.Root = binary.LittleEndian.Uint64(data[16:24])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])
}
