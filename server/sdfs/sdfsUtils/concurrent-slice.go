package sdfsutils

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
)

type ConcurrentSlice struct {
	sync.Mutex
	slice []interface{}
}

func CreateConcurrentStringSlice(slice []string) *ConcurrentSlice {
	s := make([]interface{}, 0)
	for _, element := range slice {
		s = append(s, element)
	}
	CSlice := &ConcurrentSlice{
		slice: s,
	}
	return CSlice
}

func (s *ConcurrentSlice) Append(item interface{}) {
	s.Lock()
	defer s.Unlock()
	s.slice = append(s.slice, item)
}

func (s *ConcurrentSlice) Pop(idx int) interface{} {
	var ret interface{}
	s.Lock()
	defer s.Unlock()
	if idx < len(s.slice) && idx >= 0 {
		ret = s.slice[idx]
		s.slice = append(s.slice[:idx], s.slice[idx+1:]...)
	} else {
		ret = nil
	}
	return ret
}

func (s *ConcurrentSlice) Get(idx int) interface{} {
	s.Lock()
	defer s.Unlock()
	if idx < len(s.slice) && idx >= 0 {
		return s.slice[idx]
	}
	return nil
}

func (s *ConcurrentSlice) Size() int {
	s.Lock()
	defer s.Unlock()
	return len(s.slice)
}

func (s *ConcurrentSlice) PopRandomElement() interface{} {
	s.Lock()
	defer s.Unlock()

	max := big.NewInt(int64(len(s.slice)))
	randomIndexBig, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	randomIndex := randomIndexBig.Int64()
	fmt.Printf("%d\n", randomIndex)
	randomElement := s.slice[randomIndex]
	s.slice = append(s.slice[:randomIndex], s.slice[randomIndex+1:]...)
	return randomElement
}

func (s *ConcurrentSlice) pop(idx int) interface{} {
	if idx < len(s.slice) && idx >= 0 {
		ret := s.slice[idx]
		s.slice = append(s.slice[:idx], s.slice[idx+1:]...)
		return ret
	}
	return nil
}
