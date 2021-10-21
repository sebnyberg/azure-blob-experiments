package download

var defaultConfig = &Config{
	Parallelism: 4,
	ChunkSize:   1024 * 1024 * 4, // 4 MB
}

// Config contains
type Config struct {
	// ChunkSize is the number of bytes to download per chunk.
	ChunkSize int

	// Parallelism sets the number of chunks to download in parallel.
	Parallelism int
}
