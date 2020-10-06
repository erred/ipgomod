package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"

	files "github.com/ipfs/go-ipfs-files"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/rs/zerolog"
)

type Loader struct {
	client *http.Client
	ch     chan IndexRecord
	store  CIDStorer
	api    *httpapi.HttpApi
	log    zerolog.Logger

	pool sync.Pool
}

func (l *Loader) Run(ctx context.Context, limit int) {
	sem := make(chan struct{}, limit)
	var counter int64
	for {
		select {
		case ir := <-l.ch:
			counter++
			sem <- struct{}{}
			go func(counter int64) {
				l.processIR(ctx, ir)
				<-sem
				l.log.Trace().Int64("counter", counter).Str("mod", ir.Path).Str("ver", ir.Version).Msg("done")
			}(counter)
		case <-ctx.Done():
		}
	}
}

func (l *Loader) processIR(ctx context.Context, ir IndexRecord) {
	zipbuf, ok := l.pool.Get().(*bytes.Buffer)
	if !ok {
		l.log.Error().Str("type", fmt.Sprintf("%T", zipbuf)).Msg("assert pool *bytes.Buffer")
		return
	}
	defer func() {
		zipbuf.Reset()
		l.pool.Put(zipbuf)
	}()

	zipURL := fmt.Sprintf("%s/%s/@v/%s.zip", ProxyURL, ir.Path, ir.Version)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, zipURL, nil)
	if err != nil {
		l.log.Error().Err(err).Msg("create url")
		return
	}
	res, err := l.client.Do(req)
	if err != nil {
		l.log.Error().Err(err).Str("url", zipURL).Msg("get zip")
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		l.log.Error().Int("code", res.StatusCode).Str("status", res.Status).Msg("proxy response")
		return
	}
	_, err = zipbuf.ReadFrom(res.Body)
	if err != nil {
		l.log.Error().Err(err).Str("url", zipURL).Msg("read body")
		return
	}

	zr, err := zip.NewReader(bytes.NewReader(zipbuf.Bytes()), int64(zipbuf.Len()))
	if err != nil {
		l.log.Error().Err(err).Str("url", zipURL).Msg("zip reader")
		return
	}

	// dir := fmt.Sprintf("%s/@v/%s", ir.Path, ir.Version)
	// os.MkdirAll(dir, 0o755)
	// defer func() {
	// 	os.RemoveAll(dir)
	// }()

	var fhs []FileHash
	p, err := l.api.Unixfs().Add(ctx, files.NewReaderFile(zipbuf), options.Unixfs.Pin(false))
	if err != nil {
		l.log.Error().Err(err).Str("mod", ir.Path).Str("ver", ir.Version).Msg("add unixfs zip")
		return
	}
	fhs = append(fhs, FileHash{
		Module:  ir.Path,
		Version: ir.Version,
		File:    "",
		CID:     p.Cid().String(),
	})

	for _, zf := range zr.File {
		rc, err := zf.Open()
		if err != nil {
			l.log.Error().Err(err).Str("mod", ir.Path).Str("ver", ir.Version).Str("file", zf.Name).Msg("open zip file")
			continue
		}

		p, err := l.api.Unixfs().Add(ctx, files.NewReaderFile(rc), options.Unixfs.Pin(true))
		if err != nil {
			l.log.Error().Err(err).Str("mod", ir.Path).Str("ver", ir.Version).Str("file", zf.Name).Msg("add unixfs file")
			return
		}
		fhs = append(fhs, FileHash{
			Module:  ir.Path,
			Version: ir.Version,
			File:    zf.Name,
			CID:     p.Cid().String(),
		})
	}

	err = l.store.AddFiles(ctx, fhs)
	if err != nil {
		l.log.Error().Err(err).Str("mod", ir.Path).Str("ver", ir.Version).Msg("save file hashes")
		return
	}
}
