package download

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type readAtBlob struct {
	ctx     context.Context
	blobURL azblob.BlobURL
	count   int
}

func (b *readAtBlob) ReadAt(p []byte, off int64) (n int, retErr error) {
	blobCount := len(p)
	if len(p)+int(off) > b.count { // last block
		blobCount = azblob.CountToEnd
		retErr = io.EOF
	}
	resp, downloadErr := b.blobURL.Download(b.ctx, off, int64(blobCount),
		azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if downloadErr != nil {
		return 0, downloadErr
	}
	reader := resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: 5})
	defer reader.Close()
	var ntot int
	for {
		nread, readErr := reader.Read(p[ntot:])
		ntot += nread
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return 0, readErr
		}
	}
	return ntot, retErr
}
