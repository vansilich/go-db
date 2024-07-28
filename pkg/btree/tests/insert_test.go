package btree

import (
	"testing"

	"github.com/vansilich/db/pkg/btree/tests/utils"
)

func TestInsertOne(t *testing.T) {
	c := utils.NewC()
	if err := c.Add("test_key", "test_value"); err != nil {
		t.Fatalf("Tree.Insert() has error: %s", err.Error())
	}
}

func TestInsertTwo(t *testing.T) {
	c := utils.NewC()
	var err error
	if err = c.Add("test_key_1", "test_value_1"); err != nil {
		t.Fatalf("[1] Tree.Insert() has error: %s", err.Error())
	}

	if err = c.Add("test_key_2", "test_value_2"); err != nil {
		t.Fatalf("[2] Tree.Insert() has error: %s", err.Error())
	}
}
