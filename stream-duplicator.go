package streamDuplicator

import (
	"io"
	"sync"

	"github.com/google/uuid"
)

type StreamDuplicator struct {
	source        io.ReadCloser
	cache         map[int][]byte
	readers       map[uuid.UUID]*Reader
	mu            sync.Mutex
	maxOffsetDiff int
}

func NewStreamDuplicator(source io.ReadCloser) *StreamDuplicator {
	return NewStreamDuplicatorWithMaxOffsetDiff(source, 0) // no maxOffsetDiff limit
}

func NewStreamDuplicatorWithMaxOffsetDiff(source io.ReadCloser, maxOffsetDiff int) *StreamDuplicator {
	return &StreamDuplicator{
		source:        source,
		cache:         make(map[int][]byte),
		readers:       make(map[uuid.UUID]*Reader),
		maxOffsetDiff: maxOffsetDiff,
	}
}

func NewStreamDuplicatorWithReadersAmount(source io.ReadCloser, readersAmount int) (*StreamDuplicator, []*Reader) {
	return NewStreamDuplicatorWithMaxOffsetDiffWithReadersAmount(source, 0, readersAmount) // no maxOffsetDiff limit
}

func NewStreamDuplicatorWithMaxOffsetDiffWithReadersAmount(source io.ReadCloser, maxOffsetDiff, readersAmount int) (*StreamDuplicator, []*Reader) {
	sd := &StreamDuplicator{
		source:        source,
		cache:         make(map[int][]byte),
		readers:       make(map[uuid.UUID]*Reader),
		maxOffsetDiff: maxOffsetDiff,
	}

	readers := make([]*Reader, readersAmount)
	for i := 0; i < readersAmount; i++ {
		readers[i] = sd.AddReader()
	}

	return sd, readers
}

func (sd *StreamDuplicator) AddReader() *Reader {
	sd.mu.Lock()
	id := uuid.New()
	reader := &Reader{
		id:         id,
		duplicator: sd,
		byteIndex:  0,
	}
	sd.readers[id] = reader
	sd.mu.Unlock()
	return reader
}

func (sd *StreamDuplicator) cleanCache() {
	for chunkIndex := range sd.cache {
		allRead := true

		for _, reader := range sd.readers {
			if reader.byteIndex <= chunkIndex+len(sd.cache[chunkIndex])-1 {
				allRead = false
				break
			}
		}

		if allRead {
			delete(sd.cache, chunkIndex)
		}
	}
}

func (sd *StreamDuplicator) getMinByteIndex() int {
	minIndex := int(^uint(0) >> 1) // max int
	for _, reader := range sd.readers {
		if reader.byteIndex < minIndex {
			minIndex = reader.byteIndex
		}
	}
	return minIndex
}
