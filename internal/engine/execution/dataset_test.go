package execution

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_bigModN(t *testing.T) {
	tests := []struct {
		name     string
		x        *big.Int
		n        int64
		expected int64
	}{
		{
			name:     "positive number smaller than n",
			x:        big.NewInt(5),
			n:        10,
			expected: 5,
		},
		{
			name:     "positive number larger than n",
			x:        big.NewInt(15),
			n:        10,
			expected: 5,
		},
		{
			name:     "positive number equal to n",
			x:        big.NewInt(10),
			n:        10,
			expected: 0,
		},
		{
			name:     "positive number multiple of n",
			x:        big.NewInt(20),
			n:        10,
			expected: 0,
		},
		{
			name:     "negative number",
			x:        big.NewInt(-7),
			n:        5,
			expected: 3,
		},
		{
			name:     "zero",
			x:        big.NewInt(0),
			n:        7,
			expected: 0,
		},
		{
			name:     "large number",
			x:        new(big.Int).Exp(big.NewInt(2), big.NewInt(100), nil),
			n:        17,
			expected: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bigModN(tt.x, tt.n)
			assert.Equal(t, tt.expected, result)
		})
	}
}
func Test_bytesModN(t *testing.T) {
	tests := []struct {
		name     string
		h        []byte
		o        int64
		n        int64
		expected int64
	}{
		{
			name:     "empty byte slice",
			h:        []byte{},
			n:        10,
			expected: 0,
		},
		{
			name:     "single byte",
			h:        []byte{5},
			n:        7,
			expected: 5,
		},
		{
			name:     "multiple bytes",
			h:        []byte{1, 2, 3, 4}, // 16909060
			n:        13,
			expected: 12,
		},
		{
			name:     "add",
			h:        []byte{1, 2, 3, 4}, // 16909060
			n:        13,
			o:        1, // => 16909061 (now it's a multiple of 13 i.e. 1300697*13)
			expected: 0,
		},
		{
			name:     "all zero bytes",
			h:        []byte{0, 0, 0, 0},
			n:        5,
			expected: 0,
		},
		{
			name:     "8-byte slice",
			h:        []byte{255, 255, 255, 255, 255, 255, 255, 255}, // 18446744073709551615 (max 64-bit uint)
			n:        1000,
			expected: 615,
		},
		{
			name:     "9-byte slice",
			h:        []byte{255, 255, 255, 255, 255, 255, 255, 255, 255}, // 4722366482869645213695 (bigger than a uint64)
			n:        1000,
			expected: 695,
		},
		{
			name:     "n is 1, remainder is 0",
			h:        []byte{83}, // prime
			n:        1,
			expected: 0,
		},
		{
			name:     "n is max int64",
			h:        []byte{1, 2, 3, 4},  // 16909060
			n:        9223372036854775807, // i.e. bigger than h as integer
			expected: 16909060,            // h as integer
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bytesModN2(tt.h, tt.o, tt.n)
			assert.Equal(t, tt.expected, result)

			if tt.o == 0 {
				result := bytesModN(tt.h, tt.n)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
