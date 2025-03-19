package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IliaW/url-gate/config"
	"github.com/IliaW/url-gate/internal/aws_sqs"
	"github.com/IliaW/url-gate/internal/broker"
	cacheClient "github.com/IliaW/url-gate/internal/cache"
	"github.com/IliaW/url-gate/internal/model"
	"github.com/IliaW/url-gate/internal/persistence"
	"github.com/IliaW/url-gate/internal/telemetry"
	"github.com/IliaW/url-gate/internal/worker"
	_ "github.com/lib/pq"
	"github.com/lmittmann/tint"
	"golang.org/x/time/rate"
)

var (
	cfg          *config.Config
	db           *sql.DB
	cache        cacheClient.CachedClient
	metadataRepo persistence.MetadataStorage
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg = config.MustLoad()
	setupLogger()
	metrics := telemetry.SetupMetrics(context.Background(), cfg)
	defer metrics.Close()
	db = setupDatabase()
	defer closeDatabase()
	metadataRepo = persistence.NewMetadataRepository(db)
	cache = cacheClient.NewMemcachedClient(cfg.CacheSettings)
	defer cache.Close()
	httpClient := setupHttpClient()
	kafkaDLQ := broker.NewKafkaDLQ(cfg.ServiceName, cfg.KafkaSettings.Producer)
	rateLimiter := rate.NewLimiter(rate.Every(cfg.WorkerSettings.TimeInterval), cfg.WorkerSettings.RequestsLimit)
	slog.Info("starting application on port "+cfg.Port, slog.String("env", cfg.Env))

	threadNum := parallelWorkers()
	getSqsChan := make(chan *string, threadNum*2) // double the size to avoid blocking
	sendSqsChan := make(chan *string, threadNum*2)
	kafkaChan := make(chan *model.CrawlTask, threadNum*2)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	sqs := aws_sqs.NewSQSWorker(getSqsChan, metrics.SQSMetrics, sendSqsChan, cfg, wg)
	go sqs.SQSConsumer(ctx)
	go sqs.SQSProducer()

	workerWg := &sync.WaitGroup{}
	urlGateWorker := &worker.UrlGateWorker{
		InputSqsChan:    getSqsChan,
		OutputSqsChan:   sendSqsChan,
		OutputKafkaChan: kafkaChan,
		HttpClient:      httpClient,
		RateLimiter:     rateLimiter,
		Cfg:             cfg,
		Db:              metadataRepo,
		Cache:           cache,
		Wg:              workerWg,
		KafkaDLQ:        kafkaDLQ,
		Metrics:         metrics.AppMetrics,
	}
	for i := 0; i < threadNum; i++ {
		workerWg.Add(1)
		go urlGateWorker.Run()
	}

	wg.Add(1)
	kafka := broker.NewKafkaProducer(kafkaChan, metrics.KafkaMetrics, cfg.KafkaSettings.Producer, wg)
	go kafka.Run()

	go healthCheckHandler()

	// Graceful shutdown.
	// 1. Stop SQS Consumer by system call. Close getSqsChan
	// 2. Wait till all Workers processed all messages from getSqsChan
	// 3. Close sendSqsChan and kafkaChan
	// 4. Wait till SQS Producer and Kafka Producer process all messages.
	// 5. Close database and memcached connections
	<-ctx.Done()
	slog.Info("stopping server...")
	workerWg.Wait()
	close(sendSqsChan)
	slog.Info("close sendSqsChan.")
	close(kafkaChan)
	slog.Info("close kafkaChan.")
	wg.Wait()
	slog.Info("server stopped.")
}

func setupLogger() *slog.Logger {
	envLogLevel := strings.ToLower(cfg.LogLevel)
	var slogLevel slog.Level
	err := slogLevel.UnmarshalText([]byte(envLogLevel))
	if err != nil {
		log.Printf("encountenred log level: '%s'. The package does not support custom log levels", envLogLevel)
		slogLevel = slog.LevelDebug
	}
	log.Printf("slog level overwritten to '%v'", slogLevel)
	slog.SetLogLoggerLevel(slogLevel)

	replaceAttrs := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			source.File = filepath.Base(source.File)
		}
		return a
	}

	var logger *slog.Logger
	if strings.ToLower(cfg.LogType) == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource:   true,
			Level:       slogLevel,
			ReplaceAttr: replaceAttrs}))
	} else {
		logger = slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			AddSource:   true,
			Level:       slogLevel,
			ReplaceAttr: replaceAttrs,
			NoColor: func() bool {
				if cfg.Env == "local" {
					return false
				}
				return true
			}()}))
	}

	slog.SetDefault(logger)
	logger.Debug("debug messages are enabled.")

	return logger
}

func setupDatabase() *sql.DB {
	slog.Info("connecting to the database...")
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		cfg.DbSettings.User,
		cfg.DbSettings.Password,
		cfg.DbSettings.Host,
		cfg.DbSettings.Port,
		cfg.DbSettings.Name,
	)
	database, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("failed to establish database connection.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	database.SetConnMaxLifetime(cfg.DbSettings.ConnMaxLifetime)
	database.SetMaxOpenConns(cfg.DbSettings.MaxOpenConns)
	database.SetMaxIdleConns(cfg.DbSettings.MaxIdleConns)

	maxRetry := 6
	for i := 1; i <= maxRetry; i++ {
		slog.Info("ping the database.", slog.String("attempt", fmt.Sprintf("%d/%d", i, maxRetry)))
		pingErr := database.Ping()
		if pingErr != nil {
			slog.Error("not responding.", slog.String("err", pingErr.Error()))
			if i == maxRetry {
				slog.Error("failed to establish database connection.")
				os.Exit(1)
			}
			slog.Info(fmt.Sprintf("wait %d seconds", 5*i))
			time.Sleep(time.Duration(5*i) * time.Second)
		} else {
			break
		}
	}
	slog.Info("connected to the database!")

	return database
}

func closeDatabase() {
	slog.Info("closing database connection.")
	err := db.Close()
	if err != nil {
		slog.Error("failed to close database connection.", slog.String("err", err.Error()))
	}
}

func setupHttpClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        cfg.HttpClientSettings.MaxIdleConnections,
		MaxIdleConnsPerHost: cfg.HttpClientSettings.MaxIdleConnectionsPerHost,
		MaxConnsPerHost:     cfg.HttpClientSettings.MaxConnectionsPerHost,
		IdleConnTimeout:     cfg.HttpClientSettings.IdleConnectionTimeout,
		TLSHandshakeTimeout: cfg.HttpClientSettings.TlsHandshakeTimeout,
		DialContext: (&net.Dialer{
			Timeout:   cfg.HttpClientSettings.DialTimeout,
			KeepAlive: cfg.HttpClientSettings.DialKeepAlive,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.HttpClientSettings.TlsInsecureSkipVerify,
		},
	}

	return &http.Client{
		Transport: transport,
		Timeout:   cfg.HttpClientSettings.RequestTimeout,
	}
}

// Set -1 to use all available CPUs
func parallelWorkers() int {
	customNumCPU := cfg.WorkerSettings.WorkersNum
	if customNumCPU == -1 {
		return runtime.NumCPU()
	}
	if customNumCPU <= 0 {
		slog.Error("workers number is 0 or less than -1")
		os.Exit(1)
	}

	return customNumCPU
}

func healthCheckHandler() {
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
		slog.Error("http server error", slog.String("err", err.Error()))
	}
}
