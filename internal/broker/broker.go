package broker

import (
	"sync"

	"github.com/ibnuzaman/mini-broker.git/internal/storage"
)

// Jembatan TCP dan storage
type Broker struct {
	mu     sync.Mutex
	topics map[string]*storage.Log
}
