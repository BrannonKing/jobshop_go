package main

import (
	"fmt"
	"github.com/oleiade/lane/v2"
	"math/rand"
	"sort"
	"testing"
	"time"
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
	t1 := time.Now()
	for i := 0; i < c; i += 1 {
		n := pq.element.Number
		slc2 = append(slc2, n)
		pq = pq.DeleteMin()
	}
	fmt.Printf("Time 1: %v\n", time.Since(t1))

	t1 = time.Now()
	sort.Slice(slc, func(i, j int) bool { return slc[i] < slc[j] })
	fmt.Printf("Time 2: %v\n", time.Since(t1))
	for i := 0; i < c; i += 1 {
		if slc[i] != slc2[i] {
			t.Error("Mismatch")
		}
	}

}

func TestLanePQ_DeleteMin(t *testing.T) {
	pq := lane.NewMinPriorityQueue[NumOrdered, int64]()
	c := 1000000
	slc := make([]int64, 0, c)
	for i := 0; i < c; i += 1 {
		n := rand.Int63()
		slc = append(slc, n)
		pq.Push(NumOrdered{Number: n}, n)
	}
	slc2 := make([]int64, 0, c)
	t1 := time.Now()
	for i := 0; i < c; i += 1 {
		_, n, _ := pq.Pop()
		slc2 = append(slc2, n)
	}
	fmt.Printf("Time 1: %v\n", time.Since(t1))

	t1 = time.Now()
	sort.Slice(slc, func(i, j int) bool { return slc[i] < slc[j] })
	fmt.Printf("Time 2: %v\n", time.Since(t1))
	for i := 0; i < c; i += 1 {
		if slc[i] != slc2[i] {
			t.Error("Mismatch")
		}
	}

}
