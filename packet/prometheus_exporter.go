package packet

import (
	"slices"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Define a struct for you collector that contains pointers
// to prometheus descriptors for each metric  to expose.

type engineCollector struct {
	engines               []*Engine
	registeredMetricTotal *prometheus.GaugeVec
	runningMetric         *prometheus.GaugeVec
	runningMetricTotal    *prometheus.GaugeVec

	packetCount            *prometheus.GaugeVec
	packetSkipped          *prometheus.GaugeVec
	packetFiltered         *prometheus.GaugeVec
	packetMaxBuffers       *prometheus.GaugeVec
	packetBuffersAllocated *prometheus.GaugeVec
	packetBuffersReleased  *prometheus.GaugeVec

	flowsPackets *prometheus.GaugeVec
	flowsFlows   *prometheus.GaugeVec

	decodeError    *prometheus.GaugeVec
	decodeKeyError *prometheus.GaugeVec
}

var engineLabels = []string{"engine_id"}
var tableLabels = []string{"engine_id", "table_id"}

var EngineCollector = newEngineCollector()

func init() {
	err := prometheus.Register(EngineCollector)
	if err != nil {
		panic(err)
	}
}

// You must create a constructor for you collector that
// initializes every descriptor and returns a pointer to the collector
func newEngineCollector() *engineCollector {
	return &engineCollector{
		engines: make([]*Engine, 0),
		registeredMetricTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "engine",
			Name:      "registered_total",
			Help:      "engines currently registered",
		}, nil),
		runningMetricTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "engine",
			Name:      "running_total",
			Help:      "engines currently running",
		}, nil),

		runningMetric: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "engine",
			Name:      "running",
			Help:      "engines currently running",
		}, engineLabels),

		packetCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "count",
			Help:      "packets seen",
		}, engineLabels),
		packetSkipped: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "skipped",
			Help:      "packets skipped",
		}, engineLabels),
		packetFiltered: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "filtered",
			Help:      "packets filtered",
		}, engineLabels),
		packetMaxBuffers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "buffers_max",
			Help:      "maximum buffers allocated",
		}, engineLabels),
		packetBuffersAllocated: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "buffers_allocated_total",
			Help:      "total numbers of allocated buffers",
		}, engineLabels),
		packetBuffersReleased: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "buffers_released_total",
			Help:      "total numbers of released buffers",
		}, engineLabels),

		flowsPackets: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "flows",
			Name:      "packets_total",
			Help:      "total number of packets in collected in flows",
		}, engineLabels),
		flowsFlows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "flows",
			Name:      "total",
			Help:      "total number of flows",
		}, engineLabels),

		decodeError: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "decode_errors_total",
			Help:      "total numbers of decoder errors",
		}, engineLabels),
		decodeKeyError: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ipfix",
			Subsystem: "packets",
			Name:      "decode_key_errors_total",
			Help:      "total numbers of decoder key errors",
		}, engineLabels),
	}
}

func (collector *engineCollector) getEngineLabels(engine *Engine) []string {
	if engine != nil {
		return []string{strconv.Itoa(slices.Index(collector.engines, engine))}
	}
	return nil
}

func (collector *engineCollector) Register(engine *Engine) {
	if engine == nil {
		return
	}
	collector.engines = append(collector.engines, engine)
	labels := collector.getEngineLabels(engine)
	if labels == nil {
		return
	}
	collector.registeredMetricTotal.WithLabelValues().Add(1)
	collector.runningMetric.WithLabelValues(labels...).Set(0)
}

func (collector *engineCollector) StartRun(engine *Engine) {
	if engine == nil {
		return
	}
	labels := collector.getEngineLabels(engine)
	if labels == nil {
		return
	}
	collector.runningMetric.WithLabelValues(labels...).Set(1)
	collector.runningMetricTotal.WithLabelValues().Add(1)
}

func (collector *engineCollector) EndRun(engine *Engine) {
	if engine == nil {
		return
	}
	labels := collector.getEngineLabels(engine)
	if labels == nil {
		return
	}
	collector.runningMetric.WithLabelValues(labels...).Set(0)
	collector.runningMetricTotal.WithLabelValues().Add(-1)
}

// Each and every collector must implement the Describe function.
// It essentially writes all descriptors to the prometheus desc channel.
func (collector *engineCollector) Describe(ch chan<- *prometheus.Desc) {
	//Update this section with the each metric you create for a given collector
	collector.registeredMetricTotal.Describe(ch)
	collector.runningMetricTotal.Describe(ch)
	collector.runningMetric.Describe(ch)

	collector.packetCount.Describe(ch)
	collector.packetSkipped.Describe(ch)
	collector.packetFiltered.Describe(ch)
	collector.packetMaxBuffers.Describe(ch)
	collector.packetBuffersAllocated.Describe(ch)
	collector.packetBuffersReleased.Describe(ch)

	collector.flowsPackets.Describe(ch)
	collector.flowsFlows.Describe(ch)

	collector.decodeError.Describe(ch)
	collector.decodeKeyError.Describe(ch)

}

func (collector *engineCollector) Collect(ch chan<- prometheus.Metric) {
	for _, engine := range collector.engines {
		labels := collector.getEngineLabels(engine)
		if labels == nil {
			continue
		}
		collector.packetCount.WithLabelValues(labels...).Set(float64(engine.packetStats.packets))
		collector.packetSkipped.WithLabelValues(labels...).Set(float64(engine.packetStats.skipped))
		collector.packetFiltered.WithLabelValues(labels...).Set(float64(engine.packetStats.filtered))
		collector.packetMaxBuffers.WithLabelValues(labels...).Set(float64(engine.packetStats.maxBuffers))
		collector.packetBuffersAllocated.WithLabelValues(labels...).Set(float64(engine.packetStats.buffersAllocated))
		collector.packetBuffersReleased.WithLabelValues(labels...).Set(float64(engine.packetStats.buffersReleased))

		stats := engine.flowtable.getDecodeStats()
		collector.decodeError.WithLabelValues(labels...).Set(float64(stats.decodeError))
		collector.decodeKeyError.WithLabelValues(labels...).Set(float64(stats.keyError))

		var sumPackets, sumFlows uint64

		if pft, ok := engine.flowtable.(*parallelFlowTable); ok {
			for _, table := range pft.tables {
				sumPackets += table.Stats.Packets
				sumFlows += table.Stats.Flows
			}
		} else if sft, ok := engine.flowtable.(*singleFlowTable); ok {
			sumPackets += sft.table.Stats.Packets
			sumFlows += sft.table.Stats.Flows
		}

		collector.flowsPackets.WithLabelValues(labels...).Set(float64(sumPackets))
		collector.flowsFlows.WithLabelValues(labels...).Set(float64(sumFlows))
	}

	collector.registeredMetricTotal.Collect(ch)
	collector.runningMetricTotal.Collect(ch)
	collector.runningMetric.Collect(ch)
	collector.packetCount.Collect(ch)
	collector.packetSkipped.Collect(ch)
	collector.packetFiltered.Collect(ch)
	collector.packetMaxBuffers.Collect(ch)
	collector.packetBuffersAllocated.Collect(ch)
	collector.packetBuffersReleased.Collect(ch)

	collector.flowsPackets.Collect(ch)
	collector.flowsFlows.Collect(ch)

	collector.decodeError.Collect(ch)
	collector.decodeKeyError.Collect(ch)

}
