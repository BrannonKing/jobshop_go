package main

import (
	"math/rand"
	"sort"
	"testing"
)

type NumOrdered struct {
	Number int64
}

func (n NumOrdered) LessThan(other ordered) bool {
	return n.Number < other.(NumOrdered).Number
}

func TestPriorityTree_DeleteMin(t *testing.T) {
	var pq *PriorityTree[NumOrdered]
	c := 1000000
	slc := make([]int64, 0, c)
	for i := 0; i < c; i += 1 {
		n := rand.Int63()
		slc = append(slc, n)
		pq = pq.MeldElement(NumOrdered{Number: n})
	}
	slc2 := make([]int64, 0, c)
	for i := 0; i < c; i += 1 {
		n := pq.element.Number
		slc2 = append(slc2, n)
		pq = pq.DeleteMin()
	}

	sort.Slice(slc, func(i, j int) bool { return slc[i] < slc[j] })
	for i := 0; i < c; i += 1 {
		if slc[i] != slc2[i] {
			t.Error("Mismatch")
		}
	}
}
