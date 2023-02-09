package opentelemetry

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
<<<<<<< HEAD
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
=======
>>>>>>> v1.22.4-customplugins
)

type traceService struct {
	converter *otel2influx.OtelTracesToLineProtocol
	writer    *writeToAccumulator
}

<<<<<<< HEAD
var _ ptraceotlp.GRPCServer = (*traceService)(nil)
=======
var _ ptraceotlp.Server = (*traceService)(nil)
>>>>>>> v1.22.4-customplugins

func newTraceService(logger common.Logger, writer *writeToAccumulator) *traceService {
	converter := otel2influx.NewOtelTracesToLineProtocol(logger)
	return &traceService{
		converter: converter,
		writer:    writer,
	}
}

<<<<<<< HEAD
func (s *traceService) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	err := s.converter.WriteTraces(ctx, req.Traces(), s.writer)
	return ptraceotlp.NewExportResponse(), err
=======
func (s *traceService) Export(ctx context.Context, req ptraceotlp.Request) (ptraceotlp.Response, error) {
	err := s.converter.WriteTraces(ctx, req.Traces(), s.writer)
	return ptraceotlp.NewResponse(), err
>>>>>>> v1.22.4-customplugins
}

type metricsService struct {
	converter *otel2influx.OtelMetricsToLineProtocol
	writer    *writeToAccumulator
}

<<<<<<< HEAD
var _ pmetricotlp.GRPCServer = (*metricsService)(nil)
=======
var _ pmetricotlp.Server = (*metricsService)(nil)
>>>>>>> v1.22.4-customplugins

var metricsSchemata = map[string]common.MetricsSchema{
	"prometheus-v1": common.MetricsSchemaTelegrafPrometheusV1,
	"prometheus-v2": common.MetricsSchemaTelegrafPrometheusV2,
}

func newMetricsService(logger common.Logger, writer *writeToAccumulator, schema string) (*metricsService, error) {
	ms, found := metricsSchemata[schema]
	if !found {
		return nil, fmt.Errorf("schema '%s' not recognized", schema)
	}

	converter, err := otel2influx.NewOtelMetricsToLineProtocol(logger, ms)
	if err != nil {
		return nil, err
	}
	return &metricsService{
		converter: converter,
		writer:    writer,
	}, nil
}

<<<<<<< HEAD
func (s *metricsService) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	err := s.converter.WriteMetrics(ctx, req.Metrics(), s.writer)
	return pmetricotlp.NewExportResponse(), err
=======
func (s *metricsService) Export(ctx context.Context, req pmetricotlp.Request) (pmetricotlp.Response, error) {
	err := s.converter.WriteMetrics(ctx, req.Metrics(), s.writer)
	return pmetricotlp.NewResponse(), err
>>>>>>> v1.22.4-customplugins
}

type logsService struct {
	converter *otel2influx.OtelLogsToLineProtocol
	writer    *writeToAccumulator
}

<<<<<<< HEAD
var _ plogotlp.GRPCServer = (*logsService)(nil)
=======
var _ plogotlp.Server = (*logsService)(nil)
>>>>>>> v1.22.4-customplugins

func newLogsService(logger common.Logger, writer *writeToAccumulator) *logsService {
	converter := otel2influx.NewOtelLogsToLineProtocol(logger)
	return &logsService{
		converter: converter,
		writer:    writer,
	}
}

<<<<<<< HEAD
func (s *logsService) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	err := s.converter.WriteLogs(ctx, req.Logs(), s.writer)
	return plogotlp.NewExportResponse(), err
=======
func (s *logsService) Export(ctx context.Context, req plogotlp.Request) (plogotlp.Response, error) {
	err := s.converter.WriteLogs(ctx, req.Logs(), s.writer)
	return plogotlp.NewResponse(), err
>>>>>>> v1.22.4-customplugins
}
