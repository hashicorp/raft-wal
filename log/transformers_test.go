package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressions(t *testing.T) {

	cases := []LogCompression{
		LogCompressionNone,
		LogCompressionZlib,
		LogCompressionGZip,
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("case :%v", c), func(t *testing.T) {
			input := []byte("MY ORIGINAL DATA")
			pt := persistTransformers(UserLogConfig{Compression: c})
			lt := loadTransformers(UserLogConfig{Compression: c})

			ptr, err := runTransformers(pt, input)
			require.NoError(t, err)

			ltr, err := runTransformers(lt, ptr)
			require.NoError(t, err)
			require.Equal(t, input, ltr)
		})
	}
}
