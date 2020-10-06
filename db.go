package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
)

type DBStore struct {
	dir string
	w   *csv.Writer
	ch  chan []FileHash
	log zerolog.Logger
}

func (d *DBStore) Latest(ctx context.Context, ts string) error {
	return ioutil.WriteFile(filepath.Join(d.dir, "latest"), []byte(ts), 0o644)
}

func (d *DBStore) AddFiles(ctx context.Context, fhs []FileHash) error {
	d.ch <- fhs
	return nil
}

func (d *DBStore) adder(ctx context.Context) {
	for fhs := range d.ch {
		for _, fh := range fhs {
			err := d.w.Write([]string{fh.Module, fh.Version, fh.File, fh.CID})
			if err != nil {
				d.log.Error().Err(err).Msg("write record")
			}
		}
	}
}

func (d *DBStore) Setup(ctx context.Context, dsn string) (timestamp string, err error) {
	d.ch = make(chan []FileHash)
	f, err := os.OpenFile(filepath.Join(d.dir, "cid.csv"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("Setup: open file cid.csv: %w", err)
	}
	d.w = csv.NewWriter(f)
	b, _ := ioutil.ReadFile(filepath.Join(d.dir, "latest"))
	return string(b), nil
}
