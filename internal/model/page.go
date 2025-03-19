package model

import "time"

type Page struct {
	FullURL   string    `json:"full_url"`
	ETag      string    `json:"etag,omitempty"`
	CreatedAt time.Time `json:"timestamp"`
}

type CrawlTask struct {
	URL              string `json:"url"`
	IsAllowedToCrawl bool   `json:"allowed_to_crawl"`
	Force            bool   `json:"force"`
}

type RuleApiResponse struct {
	IsAllowed  bool   `json:"is_allowed"`
	StatusCode int    `json:"status_code"` // status code from the request to the URL that rule-api received
	Error      string `json:"error"`       // error body from the request to the URL that rule-api received
}
