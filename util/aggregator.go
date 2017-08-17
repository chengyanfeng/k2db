package util

import (
	"sync"
)

var Aggr = Aggregator{Lock: sync.Mutex{}}

type Aggregator struct {
	Cache string
	Lock  sync.Mutex
}

func (this *Aggregator) Add(csv string) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.Cache += csv + "\n"
}

func (this *Aggregator) Dump() string {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	dump := this.Cache
	this.Cache = ""
	return dump
}
