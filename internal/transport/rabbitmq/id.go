package rabbitmq

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// newID returns a 128-bit random hex string. Go stdlib has no UUID package;
// this is enough for AMQP MessageId and consumer tags.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%x%x", time.Now().UnixNano(), time.Now().UnixNano()^int64(len(b)))
	}
	return hex.EncodeToString(b[:])
}
