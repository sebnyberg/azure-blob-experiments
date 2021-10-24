package azdl

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/sebnyberg/azure-blob-experiments/ioz"
)

// DownloadBlobConcurrently downloads the provided blob with up to parallelism
// number of chunks / goroutines in parallel.
func DownloadBlobConcurrently(
	ctx context.Context,
	blobURL azblob.BlobURL,
	chunkSize int,
	parallelism int,
) (io.ReadCloser, error) {
	// Fetch blob properties to count bytes
	props, err := blobURL.GetProperties(ctx,
		azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, fmt.Errorf("get blob failed, %w", err)
	}

	// Make blob satisfy io.ReaderAt
	blob := &readAtBlob{
		ctx:     ctx,
		blobURL: blobURL,
		count:   int(props.ContentLength()),
	}

	return ioz.ParallelChunkReader(ctx, blob, blob.count, chunkSize, parallelism)
}
