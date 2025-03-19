package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/IliaW/url-gate/config"
	"github.com/IliaW/url-gate/internal/model"
	"github.com/IliaW/url-gate/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress/lz4"
)

type KafkaProducerClient struct {
	kafkaChan   <-chan *model.CrawlTask
	kafkaWriter *kafka.Writer
	metrics     *telemetry.KafkaMetrics
	cfg         *config.ProducerConfig
	wg          *sync.WaitGroup
}

func NewKafkaProducer(kafkaChan <-chan *model.CrawlTask, metrics *telemetry.KafkaMetrics, cfg *config.ProducerConfig,
	wg *sync.WaitGroup) *KafkaProducerClient {
	kafkaWriter := kafka.Writer{
		Addr:         kafka.TCP(cfg.Addr...),
		Topic:        cfg.WriteTopicName,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  cfg.MaxAttempts,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: 100 * time.Millisecond,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAsks),
		Async:        cfg.Async,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				slog.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	return &KafkaProducerClient{
		kafkaChan:   kafkaChan,
		kafkaWriter: &kafkaWriter,
		metrics:     metrics,
		cfg:         cfg,
		wg:          wg,
	}
}

func (p *KafkaProducerClient) Run() {
	slog.Info("starting kafka producer...", slog.String("topic", p.cfg.WriteTopicName))
	defer func() {
		err := p.kafkaWriter.Close()
		if err != nil {
			slog.Error("failed to close kafka writer.", slog.String("err", err.Error()))
		}
	}()
	defer p.wg.Done()

	batch := make([]kafka.Message, 0, p.cfg.BatchSize)
	batchTicker := time.NewTicker(p.cfg.BatchTimeout)
	for {
		select {
		case <-batchTicker.C:
			if len(batch) == 0 {
				continue
			}
			p.writeMessage(batch)
			batch = batch[:0]
		case task, ok := <-p.kafkaChan:
			if !ok {
				if len(batch) > 0 {
					p.writeMessage(batch)
				}
				slog.Info("stopping kafka writer.")
				return
			}
			body, err := json.Marshal(task)
			if err != nil {
				slog.Error("marshaling error.", slog.String("err", err.Error()), slog.Any("task", task))
				p.metrics.FailMsgCnt(1)
				continue
			}
			batch = append(batch, kafka.Message{
				Key:   []byte(task.URL),
				Value: body,
			})
		default:
			if len(batch) >= p.cfg.BatchSize {
				p.writeMessage(batch)
				batch = batch[:0]
				batchTicker.Reset(p.cfg.BatchTimeout)
			}
		}
	}
}

func (p *KafkaProducerClient) writeMessage(batch []kafka.Message) {
	err := p.kafkaWriter.WriteMessages(context.Background(), batch...)
	if err != nil {
		slog.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
		p.metrics.FailMsgCnt(int64(len(batch)))
		return
	}
	p.metrics.SuccessMsgCnt(int64(len(batch)))
	slog.Debug("successfully sent messages to kafka.", slog.Int("batch length", len(batch)))
}
