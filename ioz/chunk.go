package ioz

import (
	"context"
	"errors"
	"io"
	"sync"
)

const (
	defaultChunkSize   = 1024 * 1024
	defaultParallelism = 4
)

type bytesChunk struct {
	idx      int
	contents []byte
}

// chunkReader loads chunks from an io.ReaderAt in parallel
type chunkReader struct {
	results  chan bytesChunk
	todo     chan bytesChunk
	readerAt io.ReaderAt

	readMtx     sync.Mutex
	resultsRing []*bytesChunk
	chunkIdx    int
	chunkOffset int

	ctx     context.Context
	cancel  context.CancelFunc
	err     error
	errOnce sync.Once
	errMtx  sync.RWMutex

	count     int
	nchunk    int
	chunkSize int
	nparallel int
}

// ParallelChunkReader returns an io.ReadCloser that reads chunks concurrently
// from the provided reader until totalCount bytes have been read, or an error
// occurs.
func ParallelChunkReader(
	ctx context.Context,
	readerAt io.ReaderAt,
	totalByteCount int,
	chunkSize int,
	parallelism int,
) (io.ReadCloser, error) {
	if totalByteCount == 0 {
		return nil, errors.New("total count must be > 0")
	}
	if parallelism == 0 {
		parallelism = defaultParallelism
	}
	if chunkSize == 0 {
		chunkSize = defaultChunkSize
	}
	nchunk := (totalByteCount-1)/chunkSize + 1
	nparallel := max(1, min(nchunk, parallelism))
	ctx, cancel := context.WithCancel(ctx)

	dl := &chunkReader{
		ctx:         ctx,
		readerAt:    readerAt,
		cancel:      cancel,
		count:       totalByteCount,
		nchunk:      nchunk,
		chunkSize:   chunkSize,
		nparallel:   nparallel,
		results:     make(chan bytesChunk, nparallel),
		resultsRing: make([]*bytesChunk, nparallel),
		todo:        make(chan bytesChunk, nparallel),
	}
	for i := 0; i < nparallel; i++ {
		dl.todo <- bytesChunk{
			idx:      i,
			contents: make([]byte, chunkSize),
		}
	}
	for i := 0; i < nparallel; i++ {
		go dl.runWorker()
	}
	return dl, nil
}

func (d *chunkReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty buffer")
	}

	// Check if done
	select {
	case <-d.ctx.Done():
		d.maybeSetErr(d.ctx.Err())
		return 0, d.getErr()
	default:
	}

	// Lock reader mutex (in case calls.Read is called concurrently)
	d.readMtx.Lock()
	defer d.readMtx.Unlock()

	// Ensure that result is available for reading
	for d.resultsRing[d.chunkIdx%d.nparallel] == nil {
		select {
		case chunk := <-d.results:
			d.resultsRing[chunk.idx%d.nparallel] = &chunk
		case <-d.ctx.Done():
			d.maybeSetErr(d.ctx.Err())
			return 0, d.getErr()
		}
	}

	// Fetch chunk
	chunk := d.resultsRing[d.chunkIdx%d.nparallel]

	// Copy from chunk to provided bytes chunk
	n := copy(b, chunk.contents[d.chunkOffset:])
	d.chunkOffset += n

	// If the current chunk has not been fully copied, return n
	if d.chunkOffset != len(chunk.contents) {
		return n, nil
	}

	// Current chunk finished copying
	// Reset chunk offset and nil-out the chunk in the ring buffer
	d.chunkOffset = 0
	d.resultsRing[d.chunkIdx%d.nparallel] = nil

	// If there is a missing chunk still, add to todo channel
	if todoIdx := d.chunkIdx + d.nparallel; todoIdx < d.nchunk {
		select {
		case d.todo <- bytesChunk{todoIdx, chunk.contents}:
		case <-d.ctx.Done():
			d.maybeSetErr(d.ctx.Err())
			return 0, d.getErr()
		}
	}
	if d.chunkIdx == d.nchunk-1 {
		// just read the last chunk, set io.EOF
		d.maybeSetErr(io.EOF)
	}
	d.chunkIdx++
	return n, nil
}

func (d *chunkReader) runWorker() {
	// Pick chunks from todo until the channel is closed
	for chunk := range d.todo {
		offset := chunk.idx * d.chunkSize
		n, err := d.readerAt.ReadAt(chunk.contents, int64(offset))
		if err != nil && !errors.Is(err, io.EOF) {
			d.maybeSetErr(err)
			return
		}
		chunk.contents = chunk.contents[:n]
		select {
		case d.results <- chunk:
		case <-d.ctx.Done():
			d.maybeSetErr(d.ctx.Err())
			return
		}
	}
}

// maybeSetErr sets d.err the first time it is called.
func (d *chunkReader) maybeSetErr(err error) {
	d.errOnce.Do(func() {
		d.errMtx.Lock()
		defer d.errMtx.Unlock()
		d.err = err
	})
	d.cancel()
}

// getErr returns the first encountered error, if any.
func (d *chunkReader) getErr() error {
	d.errMtx.RLock()
	defer d.errMtx.RUnlock()
	return d.err
}

// Close closes the downloader.
func (d *chunkReader) Close() error {
	d.maybeSetErr(errors.New("reader closed by caller"))
	if err := d.getErr(); err != io.EOF {
		return err
	}
	return nil
}
