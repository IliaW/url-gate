package persistence

import (
	"database/sql"
	"log/slog"

	"github.com/IliaW/url-gate/internal/model"
)

type MetadataStorage interface {
	GetLastCrawl(string) *model.Page
}

type MetadataRepository struct {
	db *sql.DB
}

func NewMetadataRepository(db *sql.DB) *MetadataRepository {
	return &MetadataRepository{db: db}
}

// GetLastCrawl returns the last created crawl metadata for the given URL.
func (mr *MetadataRepository) GetLastCrawl(url string) *model.Page {
	var pages []*model.Page
	rows, err := mr.db.Query("SELECT url, e_tag FROM web_crawler.crawl_metadata WHERE url = $1 ORDER BY timestamp DESC LIMIT 1", url) // get by last timestamp
	if err != nil {
		slog.Error("failed to get crawled metadata from the database.", slog.String("err", err.Error()))
		return nil
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {
			slog.Error("failed to close rows.", slog.String("err", err.Error()))
		}
	}(rows)

	for rows.Next() {
		var page model.Page
		if err = rows.Scan(&page.FullURL, &page.ETag); err != nil {
			slog.Error("failed to scan crawled metadata from the database.", slog.String("err", err.Error()))
			return nil
		}
		pages = append(pages, &page)
	}

	if err = rows.Err(); err != nil {
		slog.Error("failed to get crawled metadata from the database.", slog.String("err", err.Error()))
		return nil
	}
	if len(pages) == 0 {
		slog.Debug("no crawled metadata found for the given URL.", slog.String("url", url))
		return nil
	}
	slog.Debug("pages found.", slog.Any("size", len(pages)))
	return pages[0]
}
