package conhash

import (
	"testing"
	"encoding/json"
)

func LogJson(c *ConHash) string {
	v := struct{
		NumNodes int
		Nodes string
		NumRing int
		Ring string
	}{}
	b, _ := json.Marshal(c.nodes)
	v.Nodes = string(b)
	v.NumNodes = len(c.nodes)

	b, _ = json.Marshal(c.ring)
	v.Ring = string(b)
	v.NumRing = len(c.ring)

	b, _ = json.MarshalIndent(v, "", "  ")
	return string(b)
}

func TestConHash(t *testing.T) {
	c := NewConHash()
	t.Logf("New: %s", LogJson(c))

	c.Set("A", 1, 3)
	c.Set("B", 2, 3)
	c.Set("C", 3, 3)
	t.Logf("Set ABC: %s", LogJson(c))

	i := c.Get("A-0").(int)
	if i != 1 {
		t.Fatalf("Get A returns %d except 1", i)
    }
	i = c.Get("A-1").(int)
	if i != 1 {
		t.Fatalf("Get A returns %d except 1", i)
    }
	i = c.Get("A-2").(int)
	if i != 1 {
		t.Fatalf("Get A returns %d except 1", i)
    }

	c.Erase("B")
	c.Erase("A")
	t.Logf("Set C: %s", LogJson(c))
}
