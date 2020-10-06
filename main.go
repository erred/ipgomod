package main

import (
	"bytes"
	"context"
	"flag"
	"net/http"
	"os"
	"sync"
	"time"

	httpapi "github.com/ipfs/go-ipfs-http-client"
	"go.seankhliao.com/usvc"
)

const (
	IpfsURL       = "http://ipfs.datastore.svc.cluster.local:5001"
	IndexURL      = "https://index.golang.org/index"
	ProxyURL      = "https://proxy.golang.org"
	ParallelLimit = 10
)

func main() {
	os.Exit(run())
}

func run() int {
	lo := usvc.LoggerOpts{}
	var APIURL, dsn string

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	lo.Flags(fs)
	fs.StringVar(&APIURL, "api", IpfsURL, "api endpoint")
	fs.Parse(os.Args[1:])

	log := lo.Logger(true)
	indexchan := make(chan IndexRecord)

	c := &http.Client{}
	api, err := httpapi.NewURLApiWithClient(APIURL, c)
	if err != nil {
		log.Error().Err(err).Str("url", APIURL).Msg("create ipfs api")
		return 1
	}

	dbstore := &DBStore{
		dir: "/data",
		log: log.With().Str("module", "DBStore").Logger(),
	}

	crawler := &IndexCrawler{
		client:   http.DefaultClient,
		ch:       indexchan,
		store:    dbstore,
		interval: 1 * time.Minute,
		log:      log.With().Str("module", "IndexCrawler").Logger(),
	}

	loader := Loader{
		client: http.DefaultClient,
		ch:     indexchan,
		store:  dbstore,
		api:    api,
		log:    log.With().Str("module", "Loader").Logger(),
		pool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 1<<29)
				buf := bytes.NewBuffer(b)
				buf.Reset()
				return buf
			},
		},
	}

	ctx := context.Background()

	ts, err := dbstore.Setup(ctx, dsn)
	if err != nil {
		log.Error().Err(err).Msg("setup db")
	}
	log.Info().Str("ts", ts).Msg("initial timestamo")

	go crawler.Crawl(ctx, ts)
	go loader.Run(ctx, ParallelLimit)

	<-ctx.Done()
	return 0
}

type TSStorer interface {
	Latest(context.Context, string) error
}

type CIDStorer interface {
	AddFiles(context.Context, []FileHash) error
}

type IndexRecord struct {
	Path      string `json:"Path"`
	Version   string `json:"Version"`
	Timestamp string `json:"Timestamp"`
	// Timestamp time.Time `json:"Timestamp"`
}

type FileHash struct {
	Module  string
	Version string
	File    string
	CID     string
}
