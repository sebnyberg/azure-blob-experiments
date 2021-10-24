package ioz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParallelChunkReader(t *testing.T) {
	want := strings.Repeat("ABCDEFGHIJ", 2)
	wantReader := bytes.NewReader([]byte(want))
	for _, tc := range []struct {
		chunkSize   int
		parallelism int
	}{
		{1, 16},
		{2, len(want)},
		{2, len(want) + 2},
		{len(want), len(want) + 2},
		{len(want) + 2, len(want) + 2},
	} {
		t.Run(fmt.Sprintf("chunkSize:%v\tparallelism:%v", tc.chunkSize, tc.parallelism), func(t *testing.T) {
			reader, err := ParallelChunkReader(context.Background(), wantReader, len(want), tc.chunkSize, tc.parallelism)
			require.NoError(t, err)
			buf := make([]byte, tc.chunkSize)
			res := make([]byte, len(want))
			var ntot int
			for {
				n, err := reader.Read(buf)
				fmt.Println(string(buf))
				t.Log(string(buf))
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
				copy(res[ntot:], buf)
				ntot += n
			}
			require.Equal(t, want, string(res))
		})
	}
}
