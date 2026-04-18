package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type MetricsManager struct {
	ProduceCount uint64
	ConsumeCount uint64
	StorageSizeBytes uint64
	ActiveConnections int64
}

var (
	DefaultMetrics = &MetricsManager{}
)

func (m *MetricsManager) IncProduce() {
	atomic.AddUint64(&m.ProduceCount, 1)
}

func (m *MetricsManager) IncConsume() {
	atomic.AddUint64(&m.ConsumeCount, 1)
}

func (m *MetricsManager) SetStorageSize(bytes uint64) {
	atomic.StoreUint64(&m.StorageSizeBytes, bytes)
}

func (b *MetricsManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "# HELP gokafka_produce_total Total number of messages produced\n")
	fmt.Fprintf(w, "# TYPE gokafka_produce_total counter\n")
	fmt.Fprintf(w, "gokafka_produce_total %d\n", atomic.LoadUint64(&b.ProduceCount))

	fmt.Fprintf(w, "# HELP gokafka_consume_total Total number of messages consumed\n")
	fmt.Fprintf(w, "# TYPE gokafka_consume_total counter\n")
	fmt.Fprintf(w, "gokafka_consume_total %d\n", atomic.LoadUint64(&b.ConsumeCount))

	fmt.Fprintf(w, "# HELP gokafka_storage_bytes Total size of logs in bytes\n")
	fmt.Fprintf(w, "# TYPE gokafka_storage_bytes gauge\n")
	fmt.Fprintf(w, "gokafka_storage_bytes %d\n", atomic.LoadUint64(&b.StorageSizeBytes))
}
