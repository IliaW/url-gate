package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IliaW/url-gate/config"
	"github.com/IliaW/url-gate/internal/broker"
	"github.com/IliaW/url-gate/internal/cache"
	"github.com/IliaW/url-gate/internal/model"
	"github.com/IliaW/url-gate/internal/persistence"
	"github.com/IliaW/url-gate/internal/telemetry"
	"github.com/PuerkitoBio/purell"
	"golang.org/x/time/rate"
)

var (
	NoSuchHostError = errors.New("no such host")
)

type UrlGateWorker struct {
	InputSqsChan    <-chan *string
	OutputSqsChan   chan<- *string
	OutputKafkaChan chan<- *model.CrawlTask
	HttpClient      *http.Client
	RateLimiter     *rate.Limiter
	Cfg             *config.Config
	Db              persistence.MetadataStorage
	Cache           cache.CachedClient
	Wg              *sync.WaitGroup
	KafkaDLQ        *broker.KafkaDLQClient
	Metrics         *telemetry.AppMetrics
}

func (w *UrlGateWorker) Run() {
	defer w.Wg.Done()
	slog.Debug("start url gate worker")

	for str := range w.InputSqsChan {
		// Expected string format: {"url": "https://example.com/page", "force": false}
		var task model.CrawlTask
		if err := json.Unmarshal([]byte(*str), &task); err != nil {
			slog.Error("failed to unmarshal the url.", slog.String("url", *str),
				slog.String("err", err.Error()))
			w.KafkaDLQ.SendUrlToDLQ(*str, err)
			w.Metrics.FailedProcessedMsgCounter(1)
			continue
		}

		normUrl, err := purell.NormalizeURLString(task.URL, purell.FlagsSafe|purell.FlagSortQuery)
		if err != nil {
			slog.Error("failed to normalize the url.", slog.String("url", task.URL),
				slog.String("err", err.Error()))
			w.KafkaDLQ.SendUrlToDLQ(*str, err)
			w.Metrics.FailedProcessedMsgCounter(1)
			continue
		}
		task.URL = normUrl

		// Check memcached if the url has been crawled (Key: url-hash)
		if w.Cache.CheckIfCrawled(task.URL) {
			slog.Debug("url has been crawled. Skip crawling.", slog.String("url", task.URL))
			w.Metrics.SuccessfullyProcessedMsgCnt(1)
			continue
		}

		// Check whether the crawl in the database and retention time for s3 hasn't expired.
		// Than verify Etag has not changed.
		if crawl := w.Db.GetLastCrawl(task.URL); crawl != nil && crawl.ETag != "" && task.Force == false {
			if time.Now().Add(-time.Duration(w.Cfg.WorkerSettings.S3RetentionDays) * 24 * time.Hour).After(crawl.CreatedAt) {
				slog.Info("s3 retention period for the crawl is out.", slog.String("url", task.URL))
				goto SkipDbVerification
			}
			statusCode, err := w.etagRequest(task.URL, crawl.ETag)
			if err != nil {
				slog.Error("etag verification request failed.", slog.String("url", task.URL))
				goto SkipDbVerification
			}

			// If 304 Not Modified, skip the crawling for the url
			if statusCode == http.StatusNotModified {
				slog.Debug("etag has not changed. Skip crawling.", slog.String("url", task.URL))
				w.Metrics.SuccessfullyProcessedMsgCnt(1)
				continue
			}
		}
	SkipDbVerification:
		// Request to rule api service
		ruleUrl := fmt.Sprintf(w.Cfg.RuleApiSettings.FullURL, task.URL, w.Cfg.WorkerSettings.UserAgent)
		ruleResponse, err := w.requestToRuleApi(ruleUrl)
		if err != nil {
			if errors.Is(err, NoSuchHostError) {
				slog.Error("skip the URL. No such host error.", slog.String("url", task.URL))
			}
			w.KafkaDLQ.SendUrlToDLQ(task.URL, err)
			w.Metrics.FailedProcessedMsgCounter(1)
			continue
		}
		if ruleResponse.Blocked {
			slog.Info("domain is blocked by custom rules. Skip scraping and classification.",
				slog.String("url", task.URL))
			continue
		}
		task.IsAllowedToCrawl = ruleResponse.IsAllowed

		// Increment the threshold for the domain
		if err := w.Cache.IncrementThreshold(task.URL); err != nil {
			// If the threshold is reached or an error - put the url back to the sqs
			w.OutputSqsChan <- str
			continue
		}

		w.OutputKafkaChan <- &task
		w.Metrics.SuccessfullyProcessedMsgCnt(1)
	}
}

func (w *UrlGateWorker) etagRequest(url string, etag string) (int, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Error("failed to create a request.", slog.String("url", url),
			slog.String("err", err.Error()))
		return 0, err
	}
	req.Header.Set("If-None-Match", etag)

	resp, err := w.HttpClient.Do(req)
	if err != nil {
		slog.Error("failed to make a request to the url.", slog.String("url", url),
			slog.String("err", err.Error()))
		return 0, err
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			slog.Warn("failed to close the response body.", slog.String("err", err.Error()))
		}
	}()

	return resp.StatusCode, nil
}

// requestToRuleApi checks if the url is allowed to crawl by the rule api service.
// Some sites do not have robots.txt. Some link may be blocked or do not work. Or request may fail due to timeout.
// For the cases defaultResponse value from config.yaml (true/false) is used.
// If request fails, the blocked value is set to false.
func (w *UrlGateWorker) requestToRuleApi(ruleUrl string) (*model.RuleApiResponse, error) {
	slog.Debug("request to rule api service.")
	defaultResponse := &model.RuleApiResponse{
		IsAllowed: w.Cfg.RuleApiSettings.DefaultCrawlAllowed,
		Blocked:   false,
	}

	ruleReq, err := http.NewRequest("GET", ruleUrl, nil)
	if err != nil {
		slog.Error("failed to create a request to the rule api service.",
			slog.String("ruleUrl", ruleUrl))
		return defaultResponse, nil
	}

	// limit requests to the rule api
	err = w.RateLimiter.Wait(context.Background())
	if err != nil {
		slog.Error("rate limiter failed.", slog.String("err", err.Error()))
		return defaultResponse, nil
	}

	resp, err := w.HttpClient.Do(ruleReq)
	if err != nil {
		slog.Error("failed to make a request to the rule api service.", slog.String("err", err.Error()),
			slog.String("ruleUrl", ruleUrl))
		return defaultResponse, nil
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			slog.Error("failed to close the response body.", slog.String("err", err.Error()),
				slog.String("ruleUrl", ruleUrl))
		}
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("failed to read the response body.", slog.String("err", err.Error()),
			slog.String("ruleUrl", ruleUrl))
		return defaultResponse, nil
	}

	// wrong request parameters or most likely, there is no access to the URL, or the robots.txt file does not exist
	if !isSuccess(resp.StatusCode) {
		strBody := string(body)
		slog.Error("rule api response status code is not successful.",
			slog.String("ruleUrl", ruleUrl),
			slog.Int("status code", resp.StatusCode),
			slog.String("body", strBody))
		// filter not existing hosts
		if strings.Contains(strBody, "no such host") {
			return nil, NoSuchHostError
		}
		return defaultResponse, nil
	}

	var ruleApiResponse model.RuleApiResponse
	if err = json.Unmarshal(body, &ruleApiResponse); err != nil {
		slog.Error("rule api response unmarshalling error", slog.String("body", string(body)),
			slog.String("ruleUrl", ruleUrl))
		return defaultResponse, nil
	}

	if !isSuccess(ruleApiResponse.StatusCode) {
		slog.Error("rule api response status code is not successful.",
			slog.String("ruleUrl", ruleUrl),
			slog.Int("status code", resp.StatusCode),
			slog.String("body", string(body)))
		return defaultResponse, nil
	}

	slog.Debug("rule api response.", slog.String("ruleUrl", ruleUrl), slog.Any("response", ruleApiResponse))
	return &ruleApiResponse, nil
}

func isSuccess(statusCode int) bool {
	return statusCode >= 200 && statusCode < 300
}
