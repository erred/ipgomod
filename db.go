package main

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
)

type DBStore struct {
	pool *pgxpool.Pool
	log  zerolog.Logger
	ch   chan pgx.Batch
}

func (d *DBStore) Latest(ctx context.Context, ts string) error {
	err := ExecuteTx(ctx, d.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPSERT INTO latest (id, timestamp) VALUES (1, $1)`, ts)
		return err
	})
	if err != nil {
		return fmt.Errorf("Latest: %w", err)
	}
	return nil
}

func (d *DBStore) AddFiles(ctx context.Context, fhs []FileHash) error {
	var b pgx.Batch
	for _, fh := range fhs {
		b.Queue(`INSERT INTO hashes (module, version, file, cid) VALUES ($1, $2, $3, $4)`, fh.Module, fh.Version, fh.File, fh.CID)
	}
	d.ch <- b
	return nil
}

func (d *DBStore) adder(ctx context.Context) {
	for b := range d.ch {
		err := ExecuteTx(ctx, d.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			_, err := tx.SendBatch(ctx, &b).Exec()
			return err
		})
		if err != nil {
			d.log.Error().Err(err).Msg("adder")
		}
	}
}

func (d *DBStore) Setup(ctx context.Context, dsn string) (timestamp string, err error) {
	// connection pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("Setup parse dsn=%s: %w", dsn, err)
	}
	config.ConnConfig.Logger = zerologadapter.NewLogger(d.log)
	d.pool, err = pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return "", fmt.Errorf("dbSetup connect dsn=%s: %w", dsn, err)
	}

	d.ch = make(chan pgx.Batch)
	go d.adder(ctx)

	// ensure tables
	err = ExecuteTx(ctx, d.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS hashes (
        module  TEXT,
        version TEXT,
        file    TEXT,
        cid     TEXT
)`)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS latest (
        id              INTEGER,
        timestamp       TEXT
)`)
		if err != nil {
			return err
		}

		row := tx.QueryRow(ctx, `SELECT timestamp FROM latest LIMIT 1`)
		err = row.Scan(&timestamp)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}

		return nil
	})

	if err != nil {
		return "", fmt.Errorf("Setup ensure tables: %w", err)
	}
	return timestamp, nil
}

func ExecuteTx(ctx context.Context, pool *pgxpool.Pool, txOpts pgx.TxOptions, fn func(pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}
	return crdb.ExecuteInTx(ctx, pgxTxAdapter{tx}, func() error { return fn(tx) })
}

type pgxTxAdapter struct {
	pgx.Tx
}

func (tx pgxTxAdapter) Exec(ctx context.Context, q string, args ...interface{}) error {
	_, err := tx.Tx.Exec(ctx, q, args...)
	return err
}
