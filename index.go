package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

type IndexCrawler struct {
	client   *http.Client
	ch       chan IndexRecord
	store    TSStorer
	interval time.Duration
	log      zerolog.Logger
}

func (c *IndexCrawler) Crawl(ctx context.Context, ts string) {
	sum, counter := 0, 2000
	for {
		if counter < 2000 {
			c.log.Debug().Dur("dur", c.interval).Int("records", counter).Msg("sleeping")
			select {
			case <-time.NewTimer(c.interval).C:
			case <-ctx.Done():
				return
			}
		}

		counter = 0
		u := IndexURL
		if ts != "" {
			u += "?since=" + ts
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			c.log.Error().Err(err).Str("url", u).Msg("create request")
			continue
		}

		res, err := c.client.Do(r)
		if err != nil {
			c.log.Error().Err(err).Str("url", u).Msg("do request")
			continue
		}
		d := json.NewDecoder(res.Body)
		for d.More() {
			var ir IndexRecord
			err = d.Decode(&ir)
			if err != nil {
				break
			}
			counter++
			sum++
			ts = ir.Timestamp

			// send somewhere
			select {
			case c.ch <- ir:
			case <-ctx.Done():
				return
			}
		}
		res.Body.Close()
		if err != nil {
			c.log.Error().Err(err).Msg("decode json")
			continue
		}

		err = c.store.Latest(ctx, ts)
		if err != nil {
			c.log.Error().Err(err).Msg("store latest")
		}

		c.log.Info().Int("records", counter).Int("sum", sum).Msg("index got")
	}
}
