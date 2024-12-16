package streamDuplicator

import (
	"errors"

	"github.com/google/uuid"
)

var ErrReachedMaxOffsetDiff = errors.New("reached max offset difference")

type Reader struct {
	id         uuid.UUID
	duplicator *StreamDuplicator
	byteIndex  int
	closed     bool
}

func NewReader(id uuid.UUID, duplicator *StreamDuplicator) *Reader {
	return &Reader{
		id:         id,
		duplicator: duplicator,
		byteIndex:  0,
		closed:     false,
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	sd := r.duplicator
	totalRead := 0
	toRead := len(p)

	if sd.maxOffsetDiff > 0 {
		sd.mu.Lock()
		minByteIndex := sd.getMinByteIndex()
		sd.mu.Unlock()

		if minByteIndex+sd.maxOffsetDiff <= r.byteIndex {
			return 0, ErrReachedMaxOffsetDiff
		}

		toRead = min(toRead, minByteIndex+sd.maxOffsetDiff-r.byteIndex)
		p = p[:toRead]
	}

	for totalRead < toRead {
		sd.mu.Lock()
		chunk, _, chunkOffset := r.findCacheChunk()
		sd.mu.Unlock()

		if chunk != nil {
			bytesToCopy := min(len(chunk)-chunkOffset, toRead-totalRead)
			copy(p[totalRead:], chunk[chunkOffset:chunkOffset+bytesToCopy])
			totalRead += bytesToCopy
			r.byteIndex += bytesToCopy

			sd.mu.Lock()
			sd.cleanCache()
			sd.mu.Unlock()
		} else { // no cache
			buf := make([]byte, toRead-totalRead)
			n, err := sd.source.Read(buf)
			if n > 0 {
				sd.mu.Lock()
				sd.cache[r.byteIndex] = buf[:n]
				sd.mu.Unlock()

				copy(p[totalRead:], buf[:n])
				totalRead += n
				r.byteIndex += n
			}
			if err != nil {
				return totalRead, err
			}
		}
	}

	return totalRead, nil
}

func (r *Reader) Close() error {
	sd := r.duplicator
	sd.mu.Lock()
	if r.closed {
		sd.mu.Unlock()
		return nil
	}
	r.closed = true
	delete(sd.readers, r.id)
	sd.mu.Unlock()
	return nil
}

func (r *Reader) findCacheChunk() ([]byte, int, int) {
	sd := r.duplicator
	for chunkIndex, chunk := range sd.cache {
		if chunkIndex <= r.byteIndex && r.byteIndex < chunkIndex+len(chunk) {
			return chunk, chunkIndex, r.byteIndex - chunkIndex
		}
	}
	return nil, 0, 0
}
