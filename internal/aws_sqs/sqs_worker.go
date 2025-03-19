package aws_sqs

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/IliaW/url-gate/config"
	"github.com/IliaW/url-gate/internal/telemetry"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	crd "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSWorker struct {
	client      *sqs.Client
	url         *string
	getSqsChan  chan<- *string
	sendSqsChan <-chan *string
	metrics     *telemetry.SQSMetrics
	cfg         *config.Config
	wg          *sync.WaitGroup
}

func NewSQSWorker(getSqsChan chan<- *string, metrics *telemetry.SQSMetrics, sendSqsChan <-chan *string,
	cfg *config.Config, wg *sync.WaitGroup) *SQSWorker {
	slog.Info("connecting to sqs...")

	c, err := connect(cfg)
	if err != nil {
		slog.Error("failed to connect to sqs.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	queueUrl, err := c.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &cfg.SQSSettings.QueueName})
	if err != nil {
		slog.Error("failed to get queue url.", slog.String("err", err.Error()),
			slog.String("queue_name", cfg.SQSSettings.QueueName))
		os.Exit(1)
	}

	sqsClient := SQSWorker{
		client:      c,
		url:         queueUrl.QueueUrl,
		getSqsChan:  getSqsChan,
		sendSqsChan: sendSqsChan,
		metrics:     metrics,
		cfg:         cfg,
		wg:          wg,
	}

	return &sqsClient
}

func (w *SQSWorker) SQSConsumer(ctx context.Context) {
	defer w.wg.Done()
	slog.Info("starting sqs consumer...", slog.String("queue_url", *w.url))

	getInput := &sqs.ReceiveMessageInput{
		QueueUrl:            w.url,
		MaxNumberOfMessages: w.cfg.SQSSettings.MaxNumberOfMessages,
		WaitTimeSeconds:     w.cfg.SQSSettings.WaitTimeSeconds,
		VisibilityTimeout:   w.cfg.SQSSettings.VisibilityTimeout,
	}
	deleteInput := &sqs.DeleteMessageBatchInput{
		QueueUrl: w.url,
		Entries:  nil,
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping sqs consumer...")
			close(w.getSqsChan)
			slog.Info("close getSqsChan.")
			return
		default:
			output, err := w.client.ReceiveMessage(ctx, getInput)
			if err != nil {
				slog.Error("failed to receive message from sqs.", slog.String("err", err.Error()))
				continue
			}
			if len(output.Messages) == 0 {
				slog.Debug("no messages received from sqs.")
				continue
			}

			// Sending messages to getSqsChan
			entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(output.Messages))
			for _, m := range output.Messages {
				w.getSqsChan <- m.Body
				entries = append(entries, types.DeleteMessageBatchRequestEntry{
					Id:            m.MessageId,
					ReceiptHandle: m.ReceiptHandle,
				})
			}
			// Deleting messages from sqs
			deleteInput.Entries = entries
			slog.Debug("deleting messages from sqs.", slog.Int("size", len(entries)))
			_, err = w.client.DeleteMessageBatch(context.Background(), deleteInput)
			if err != nil {
				slog.Error("failed to delete messages from sqs.", slog.String("err", err.Error()))
				w.metrics.FailMsgCnt(int64(len(entries))) // messages still can be processed
			} else {
				w.metrics.SuccessMsgCnt(int64(len(entries)))
			}
		}
	}
}

func (w *SQSWorker) SQSProducer() {
	defer w.wg.Done()
	slog.Info("starting sqs producer...", slog.String("queue_url", *w.url))

	ctx := context.Background()
	sendMessage := &sqs.SendMessageInput{
		QueueUrl:    w.url,
		MessageBody: nil,
	}

	// NOTE: If the threshold is reached in Memcached and another message with the same URL is added
	// to EMPTY queue, you will be stuck in a loop of getSqsMessage -> error: threshold reached -> sendSqsMessage
	// until the cache times out or the threshold decrease. It is okay and may mostly happen during testing.
	for m := range w.sendSqsChan {
		sendMessage.MessageBody = m
		slog.Debug("sending message back to sqs.", slog.String("message", *m))
		_, err := w.client.SendMessage(ctx, sendMessage)
		w.metrics.SentBackToSqsMsgCnt(1)
		if err != nil {
			slog.Error("failed to send message to sqs.", slog.String("message", *m),
				slog.String("err", err.Error()))
		}
	}
	slog.Info("stopping sqs producer.")
}

func connect(cfg *config.Config) (*sqs.Client, error) {
	sqsConfig, err := awsCfg.LoadDefaultConfig(context.Background(), awsCfg.WithRegion(cfg.SQSSettings.Region))
	if err != nil {
		slog.Error("failed to load sqs config.", slog.String("err", err.Error()))
		return nil, err
	}

	if cfg.Env == "local" {
		sqsConfig.BaseEndpoint = &cfg.SQSSettings.AwsBaseEndpoint // for LocalStack
		sqsConfig.Credentials = crd.NewStaticCredentialsProvider("test", "test", "")
	}

	return sqs.NewFromConfig(sqsConfig), nil
}
