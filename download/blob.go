package download

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type blob struct {
	ctx   context.Context
	url   azblob.BlobURL
	count int
}

func (b *blob) ReadAt(p []byte, off int64) (n int, err error) {
	blobCount := b.count
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
		ntot += nread
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}
	}
	if err != nil {
		return 0, err
	}
	return ntot, nil
}
