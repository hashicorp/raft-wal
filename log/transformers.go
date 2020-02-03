package log

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
)

type transformer func([]byte) ([]byte, error)

func runTransformers(transformers []transformer, data []byte) ([]byte, error) {
	var err error
	for _, tr := range transformers {
		data, err = tr(data)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func persistTransformers(config LogConfig) []transformer {
	tr := []transformer{}
	if config.Compression != LogCompressionNone {
		tr = append(tr, compress(config.Compression))
	}
	return tr
}

func loadTransformers(config LogConfig) []transformer {
	tr := []transformer{}
	if config.Compression != LogCompressionNone {
		tr = append(tr, uncompress(config.Compression))
	}
	return tr
}

func uncompress(compressionType LogCompression) transformer {
	return func(data []byte) ([]byte, error) {
		if compressionType == LogCompressionNone {
			return data, nil
		}

		var buf bytes.Buffer
		var reader io.ReadCloser
		var err error

		switch compressionType {
		case LogCompressionZlib:
			reader, err = zlib.NewReader(bytes.NewReader(data))
		case LogCompressionGZip:
			reader, err = gzip.NewReader(bytes.NewReader(data))
		default:
			return nil, fmt.Errorf("unknown compression typoe: %v", compressionType)
		}

		if err != nil {
			return nil, err
		}

		if _, err = io.Copy(&buf, reader); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}
}

func compress(compressionType LogCompression) transformer {
	return func(data []byte) ([]byte, error) {
		var buf bytes.Buffer
		var writer io.WriteCloser
		switch compressionType {
		case LogCompressionNone:
			return data, nil
		case LogCompressionZlib:
			writer = zlib.NewWriter(&buf)
		case LogCompressionGZip:
			writer = gzip.NewWriter(&buf)
		default:
			return nil, fmt.Errorf("unknown compression typoe: %v", compressionType)
		}

		if _, err := writer.Write(data); err != nil {
			return nil, err
		}

		if err := writer.Close(); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}
}
