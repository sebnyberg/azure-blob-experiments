package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

type bytesChunk struct {
	idx      int
	contents []byte
}

// chunkDownloader loads chunks from Azure Blob storage in parallel, retaining
// the original order.
type chunkDownloader struct {
	results  chan bytesChunk
	todo     chan bytesChunk
	readerAt io.ReaderAt

	readMtx     sync.Mutex
	resultsRing []*bytesChunk
	chunkIdx    int
	chunkOffset int

	done    bool
	ctx     context.Context
	cancel  context.CancelFunc
	err     chan error
	errOnce sync.Once

	count     int
	nchunk    int
	chunkSize int
	nparallel int
}

func ParallelChunkReader(
	ctx context.Context,
	readerAt io.ReaderAt,
	totalCount int,
	config *Config,
) (io.ReadCloser, error) {
	if config == nil {
		config = defaultConfig
	}
	nchunk := (totalCount-1)/config.ChunkSize + 1
	nparallel := max(1, min(nchunk, config.Parallelism))
	ctx, cancel := context.WithCancel(ctx)

	dl := &chunkDownloader{
		ctx:         ctx,
		readerAt:    readerAt,
		cancel:      cancel,
		count:       totalCount,
		nchunk:      nchunk,
		chunkSize:   config.ChunkSize,
		nparallel:   nparallel,
		results:     make(chan bytesChunk, nparallel),
		resultsRing: make([]*bytesChunk, nparallel),
		todo:        make(chan bytesChunk, nparallel),
		err:         make(chan error),
	}
	for i := 0; i < int(nparallel); i++ {
		dl.todo <- bytesChunk{
			idx:      i,
			contents: make([]byte, config.ChunkSize),
		}
	}
	for i := 0; i < int(nparallel); i++ {
		go dl.runWorker()
	}
	return dl, nil
}

func (d *chunkDownloader) Read(b []byte) (int, error) {
	d.readMtx.Lock()
	defer d.readMtx.Unlock()
	if d.done {
		return 0, io.EOF
	}
	// Ensure that result is available for reading
	for d.resultsRing[d.chunkIdx%d.nparallel] == nil {
		select {
		case chunk := <-d.results:
			fmt.Println("read chunk ", chunk.idx, " from results channel")
			d.resultsRing[chunk.idx%d.nparallel] = &chunk
		case <-d.ctx.Done():
			return 0, d.ctx.Err()
		case err := <-d.err:
			d.err <- err
			return 0, err
		}
	}

	// Fetch chunk
	chunk := d.resultsRing[d.chunkIdx%d.nparallel]

	// Copy from chunk to provided bytes chunk
	n := copy(b, chunk.contents[d.chunkOffset:])
	d.chunkOffset += n

	// If the current chunk has been copied
	if d.chunkOffset == len(chunk.contents) {

		// Reset chunk offset and nil-out the chunk in the ring buffer
		d.chunkOffset = 0
		d.resultsRing[d.chunkIdx%d.nparallel] = nil

		// If there is a missing chunk still
		if todoIdx := d.chunkIdx + d.nparallel; todoIdx < d.nchunk {
			select {
			case d.todo <- bytesChunk{todoIdx, chunk.contents}:
				fmt.Println("put chunk ", todoIdx, " into todo channel")
			case <-d.ctx.Done():
				return 0, d.ctx.Err()
			case err := <-d.err:
				d.err <- err
				return 0, err
			}
		}
		if d.chunkIdx == d.nchunk-1 {
			d.done = true
			close(d.todo)
			close(d.results)
		}
		d.chunkIdx++
	}
	return n, nil
}

func (d *chunkDownloader) setErr(err error) {
	d.errOnce.Do(func() {
		d.err <- err
		d.cancel()
	})
}

func (d *chunkDownloader) runWorker() {
	// Pick chunks from todo until the channel is closed
	for chunk := range d.todo {
		fmt.Println("read chunk ", chunk.idx, " from todo channel")
		offset := chunk.idx * d.chunkSize
		n, err := d.readerAt.ReadAt(chunk.contents, int64(offset))
		if err != nil && !errors.Is(err, io.EOF) {
			d.setErr(err)
			return
		}
		chunk.contents = chunk.contents[:n]
		select {
		case d.results <- chunk:
			fmt.Println("put chunk ", chunk.idx, " into results channel")
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *chunkDownloader) Close() error {
	for d.results != nil && d.todo != nil {
		select {
		case _, open := <-d.results:
			if !open {
				d.results = nil
			} else {
				close(d.results)
			}
		case _, open := <-d.todo:
			if !open {
				d.results = nil
			} else {
				close(d.todo)
			}
		}
	}
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
