package download

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type Blob struct {
	ctx context.Context
	// name  string
	url   azblob.BlobURL
	count int
}

func (b *Blob) ReadAt(p []byte, off int64) (n int, err error) {
	blobCount := n
	if len(p)+int(off) > b.count { // last block
		blobCount = azblob.CountToEnd
	}
	resp, err := b.url.Download(b.ctx, off, int64(blobCount),
		azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}
	reader := resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: 5})
	defer reader.Close()
	var ntot, nread int
	for {
		nread, err = reader.Read(p[ntot:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}
		ntot += nread
	}
	if err != nil {
		return 0, err
	}
	return ntot, nil
}
