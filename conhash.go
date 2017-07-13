package conhash

/*
#include <stdint.h>
#include <stdlib.h>

inline uint32_t StringHash(char const* str, int size)
{
  uint32_t ret = 0;
  int i = 0;
  for (; i<size; ++i)
  {
    uint32_t ch = (uint32_t)*str++;
    ret = ret * 131 + ch;
  }
  return ret;
}
*/
import "C"
import "unsafe"

import (
	"sync"
	"fmt"
	"crypto/md5"
)

type ConHash struct {
	nodes map[string]*node
	ring []*virtualNode
	mu sync.RWMutex
}

type node struct {
	Key string
	Value interface{}
	Virtual int
}

type virtualNode struct {
	HashCode uint32
	Nd *node
}

func NewConHash() *ConHash {
    return &ConHash{
		nodes : make(map[string]*node),
		ring : make([]*virtualNode, 0),
    }
}

func (this *ConHash) Set(key string, value interface{}, virtual int) {
	this.mu.Lock()
	defer this.mu.Unlock()
	nd := &node {
		Key : key,
		Value : value,
		Virtual : virtual,
    }

	_, exists := this.nodes[key]
	if exists {
		this.eraseWithoutLock(key)
	}

	this.nodes[key] = nd
	for i := 0; i < virtual; i++ {
		hashCode := this.hash(fmt.Sprintf("%s-%d", key, i))
		this.insert(nd, hashCode)
	}
}

func (this *ConHash) Erase(key string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.eraseWithoutLock(key)
}

func (this *ConHash) Get(key string) interface{} {
	this.mu.RLock()
	defer this.mu.RUnlock()

	i := this.getNodeIndex(this.hash(key))
	vnd := this.ring[i]
	return vnd.Nd.Value
}

func (this *ConHash) eraseWithoutLock(key string) {
	nd, exists := this.nodes[key]
	if !exists {
		return
	}

	for i := 0; i < nd.Virtual; i++ {
		hashCode := this.hash(fmt.Sprintf("%s-%d", key, i))
		index := this.lowerBound(hashCode)
		if index < len(this.ring) && this.ring[index].Nd == nd {
			this.ring = append(this.ring[:index], this.ring[index+1:]...)
        }
	}

	delete(this.nodes, key)
}

func (this *ConHash) insert(nd *node, hashCode uint32) {
	i := this.lowerBound(hashCode)
	vnd := &virtualNode{HashCode : hashCode, Nd : nd}
	this.ring = append(this.ring, nil)
	copy(this.ring[i+1:], this.ring[i:len(this.ring)-1])
	this.ring[i] = vnd
}

func (this *ConHash) getNodeIndex(hashCode uint32) int {
	i := this.lowerBound(hashCode)
	if i == len(this.ring) {
		i = 0
	}
	return i
}

// lower bound search
// * Copy from C++ STL.algorithm.lowerbound functional.
func (this *ConHash) lowerBound(hashCode uint32) int {
	left := 0
	size := len(this.ring)
	for size > 0 {
		half := size >> 1
		middle := left + half
		if this.ring[middle].HashCode < hashCode {
			left = middle + 1
			size = size - half - 1
        } else {
			size = half
		}
    }
	return left
}

func (this *ConHash) hash(key string) uint32 {
	key = fmt.Sprintf("%x", md5.Sum([]byte(key)))
	var ckey *C.char = C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	return uint32(C.StringHash(ckey, (C.int)(len(key))))
}

