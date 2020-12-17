package util

import (
	"bytes"
)

func FillBuffer(b *bytes.Buffer, length int) *bytes.Buffer{
	if b.Len() < length {
		appendBuf := make([]byte, length - b.Len())
		b.Write(appendBuf)
	}

	return b
}
