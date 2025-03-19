package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/detectors/aws/ecs"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/IliaW/url-gate/config"
	"github.com/google/uuid"
)

var meter metric.Meter

type MetricsProvider struct {
	KafkaMetrics *KafkaMetrics
	AppMetrics   *AppMetrics
	SQSMetrics   *SQSMetrics
	Close        func()
}

type KafkaMetrics struct {
	SuccessMsgCnt func(count int64)
	FailMsgCnt    func(count int64)
}

type AppMetrics struct {
	SuccessfullyProcessedMsgCnt func(count int64)
	FailedProcessedMsgCounter   func(count int64)
}

type SQSMetrics struct {
	SuccessMsgCnt       func(count int64)
	FailMsgCnt          func(count int64)
	SentBackToSqsMsgCnt func(count int64)
}

func SetupMetrics(ctx context.Context, cfg *config.Config) *MetricsProvider {
	metricsProvider := new(MetricsProvider)
	var meterProvider *sdkmetric.MeterProvider

	if cfg.TelemetrySettings.Enabled {
		r, err := newResource(cfg)
		if err != nil {
			slog.Error("failed to get resource.", slog.String("err", err.Error()))
			os.Exit(1)
		}
		exporter, err := newMetricExporter(ctx, cfg.TelemetrySettings)
		if err != nil {
			slog.Error("failed to get metric exporter.", slog.String("err", err.Error()))
			os.Exit(1)
		}
		meterProvider = newMeterProvider(exporter, *r)
		otel.SetMeterProvider(meterProvider)
	}

	meter = otel.Meter(cfg.ServiceName)
	metricsProvider.Close = func() {
		if meterProvider != nil {
			err := meterProvider.Shutdown(ctx)
			if err != nil {
				slog.Error("failed to shutdown metrics provider.", slog.String("err", err.Error()))
			}
		}
	}

	// Set up kafka metrics
	kafkaSuccessCounter, err := meter.Int64Counter("url-gate.kafka.send.success",
		metric.WithDescription("The number of messages that the kafka successfully processed"),
		metric.WithUnit("{messages}"))
	kafkaFailCounter, err := meter.Int64Counter("url-gate.kafka.send.fail",
		metric.WithDescription("The number of messages that the kafka could not process"),
		metric.WithUnit("{messages}"))
	if err != nil {
		slog.Error("failed to create telemetry counters for kafka.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	metricsProvider.KafkaMetrics = &KafkaMetrics{
		SuccessMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				kafkaSuccessCounter.Add(ctx, count)
			}
		},
		FailMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				kafkaFailCounter.Add(ctx, count)
			}
		},
	}

	// Set up worker metrics
	appSuccessCounter, err := meter.Int64Counter("url-gate.messages.success",
		metric.WithDescription("The number of messages that the worker successfully processed"),
		metric.WithUnit("{messages}"))
	appFailCounter, err := meter.Int64Counter("url-gate.messages.fail",
		metric.WithDescription("The number of messages that the worker could not be processed. The messages send to DLQ."),
		metric.WithUnit("{messages}"))
	if err != nil {
		slog.Error("failed to create telemetry counters fo worker.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	metricsProvider.AppMetrics = &AppMetrics{
		SuccessfullyProcessedMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				appSuccessCounter.Add(ctx, count)
			}
		},
		FailedProcessedMsgCounter: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				appFailCounter.Add(ctx, count)
			}
		},
	}

	// Set up sqs metrics
	sqsSuccessCounter, err := meter.Int64Counter("url-gate.sqs.receive.success",
		metric.WithDescription("The number of messages that the sqs worker successfully processed"),
		metric.WithUnit("{messages}"))
	sqsFailCounter, err := meter.Int64Counter("url-gate.sqs.receive.fail",
		metric.WithDescription("The number of messages that the sqs worker could not process"),
		metric.WithUnit("{messages}"))
	sentBackCounter, err := meter.Int64Counter("url-gate.sqs.sent.back",
		metric.WithDescription("The number of messages that the sqs worker sent back to sqs"),
		metric.WithUnit("{messages}"))
	if err != nil {
		slog.Error("failed to create telemetry counters fo sqs.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	metricsProvider.SQSMetrics = &SQSMetrics{
		SuccessMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				sqsSuccessCounter.Add(ctx, count)
			}
		},
		FailMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				sqsFailCounter.Add(ctx, count)
			}
		},
		SentBackToSqsMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				sentBackCounter.Add(ctx, count)
			}
		},
	}

	// initialize metrics for setup UI
	if cfg.TelemetrySettings.Enabled {
		metricsProvider.SQSMetrics.SuccessMsgCnt(1)
		metricsProvider.SQSMetrics.FailMsgCnt(1)
		metricsProvider.SQSMetrics.SentBackToSqsMsgCnt(1)
		metricsProvider.AppMetrics.SuccessfullyProcessedMsgCnt(1)
		metricsProvider.AppMetrics.FailedProcessedMsgCounter(1)
		metricsProvider.KafkaMetrics.SuccessMsgCnt(1)
		metricsProvider.KafkaMetrics.FailMsgCnt(1)
	}

	return metricsProvider
}

func newResource(cfg *config.Config) (*resource.Resource, error) {
	ecsResourceDetector := ecs.NewResourceDetector()
	ecsResource, err := ecsResourceDetector.Detect(context.Background())
	if err != nil {
		slog.Error("ecs detection failed", slog.String("err", err.Error()))
	}
	mergedResource, err := resource.Merge(ecsResource, resource.Default())
	if err != nil {
		slog.Error("failed to merge resources", slog.String("err", err.Error()))
	}
	keyValue, found := ecsResource.Set().Value("container.id")
	var serviceId string
	if found {
		serviceId = keyValue.AsString()
	} else {
		serviceId = uuid.New().String()
	}
	return resource.Merge(mergedResource,
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.DeploymentEnvironment(cfg.Env),
			semconv.ServiceInstanceID(serviceId),
		))
}

func newMetricExporter(ctx context.Context, cfg *config.TelemetryConfig) (sdkmetric.Exporter, error) {
	return otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(cfg.CollectorUrl),
		otlpmetrichttp.WithInsecure())
}

func newMeterProvider(meterExporter sdkmetric.Exporter, resource resource.Resource) *sdkmetric.MeterProvider {
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(meterExporter)),
		sdkmetric.WithResource(&resource),
	)
	return meterProvider
}
