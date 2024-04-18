package dq

import (
	"bytes"
	"strconv"
	"time"
)

func wrap(body []byte, at time.Time) []byte {
	var builder bytes.Buffer
	builder.WriteString(strconv.FormatInt(at.UnixNano(), 10))
	builder.WriteByte(timeSep)
	builder.Write(body)
	return builder.Bytes()
}
