package util

import (
	"sync"
)

var Aggr = Aggregator{Lock: sync.Mutex{}, Cache: []string{}}

type Aggregator struct {
	Cache []string
	Lock  sync.Mutex
}

func (this *Aggregator) Add(csv string) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.Cache = append(this.Cache, csv)
}

func (this *Aggregator) Dump() []string {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	dump := []string{}
	for _, v := range this.Cache {
		dump = append(dump, v)
	}
	this.Cache = []string{}
	return dump
}

func (this *Aggregator) Size() int {
	return len(this.Cache)
}
