/*************************************************************************
 * Copyright 2021 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gravwell/gravwell/v3/ingest"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/gravwell/gravwell/v3/ingesters/utils"
	"github.com/gravwell/gravwell/v3/ingesters/version"
)

const (
	defaultConfigLoc = `/opt/gravwell/etc/macosLog.conf`
	ingesterName     = `macosLog`

	PERIOD      = time.Second
	READ_PERIOD = time.Second
)

var (
	confLoc        = flag.String("config-file", defaultConfigLoc, "Location for configuration file")
	stderrOverride = flag.String("stderr", "", "Redirect stderr to a shared memory file")
	ver            = flag.Bool("version", false, "Print the version information and exit")

	lg   *log.Logger
	igst *ingest.IngestMuxer
)

func init() {
	flag.Parse()
	if *ver {
		version.PrintVersion(os.Stdout)
		ingest.PrintVersion(os.Stdout)
		os.Exit(0)
	}
	var fp string
	var err error
	if *stderrOverride != `` {
		fp = filepath.Join(`/dev/shm/`, *stderrOverride)
	}
	cb := func(w io.Writer) {
		version.PrintVersion(w)
		ingest.PrintVersion(w)
	}
	if lg, err = log.NewStderrLoggerEx(fp, cb); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stderr logger: %v\n", err)
		os.Exit(-1)
	}
}

func main() {
	debug.SetTraceback("all")

	// config setup

	cfg, err := GetConfig(*confLoc)
	if err != nil {
		lg.FatalCode(0, "Failed to get configuration: %v\n", err)
		return
	}

	if len(cfg.Global.Log_File) > 0 {
		fout, err := os.OpenFile(cfg.Global.Log_File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			lg.FatalCode(0, "Failed to open log file %s: %v", cfg.Global.Log_File, err)
		}
		if err = lg.AddWriter(fout); err != nil {
			lg.Fatal("Failed to add a writer: %v", err)
		}
		if len(cfg.Global.Log_Level) > 0 {
			if err = lg.SetLevelString(cfg.Global.Log_Level); err != nil {
				lg.FatalCode(0, "Invalid Log Level \"%s\": %v", cfg.Global.Log_Level, err)
			}
		}
	}

	tag := cfg.Global.Tag_Name

	conns, err := cfg.Global.Targets()
	if err != nil {
		lg.FatalCode(0, "Failed to get backend targets from configuration: %v\n", err)
		return
	}
	lmt, err := cfg.Global.RateLimit()
	if err != nil {
		lg.FatalCode(0, "Failed to get rate limit from configuration: %v\n", err)
		return
	}

	// create ingest connection(s)

	id, ok := cfg.Global.IngesterUUID()
	if !ok {
		lg.FatalCode(0, "Couldn't read ingester UUID\n")
	}
	igCfg := ingest.UniformMuxerConfig{
		IngestStreamConfig: cfg.Global.IngestStreamConfig,
		Destinations:       conns,
		Tags:               []string{tag},
		Auth:               cfg.Global.Secret(),
		LogLevel:           cfg.Global.LogLevel(),
		VerifyCert:         !cfg.Global.InsecureSkipTLSVerification(),
		IngesterName:       ingesterName,
		IngesterVersion:    version.GetVersion(),
		IngesterUUID:       id.String(),
		IngesterLabel:      cfg.Global.Label,
		RateLimitBps:       lmt,
		Logger:             lg,
		CacheDepth:         cfg.Global.Cache_Depth,
		CachePath:          cfg.Global.Ingest_Cache_Path,
		CacheSize:          cfg.Global.Max_Ingest_Cache,
		CacheMode:          cfg.Global.Cache_Mode,
		LogSourceOverride:  net.ParseIP(cfg.Global.Log_Source_Override),
	}
	igst, err = ingest.NewUniformMuxer(igCfg)
	if err != nil {
		lg.Fatal("Failed build our ingest system: %v\n", err)
		return
	}

	defer igst.Close()

	if err := igst.Start(); err != nil {
		lg.Fatal("Failed start our ingest system: %v\n", err)
		return
	}

	if err := igst.WaitForHot(cfg.Global.Timeout()); err != nil {
		lg.FatalCode(0, "Timedout waiting for backend connections: %v\n", err)
		return
	}

	// prepare the configuration we're going to send upstream
	err = igst.SetRawConfiguration(cfg)
	if err != nil {
		lg.FatalCode(0, "Failed to set configuration for ingester state messages\n")
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	var src net.IP

	if cfg.Global.Source_Override != `` {
		// global override
		src = net.ParseIP(cfg.Global.Source_Override)
		if src == nil {
			lg.FatalCode(0, "Global Source-Override is invalid")
		}
	}

	t, err := igst.GetTag(cfg.Global.Tag_Name)
	if err != nil {
		lg.Fatal("Failed to resolve tag \"%s\": %v\n", cfg.Global.Tag_Name, err)
	}
	go run(t, src, &wg, ctx)

	// listen for signals so we can close gracefully

	utils.WaitForQuit()

	cancel()

	if err := igst.Sync(time.Second); err != nil {
		lg.Error("Failed to sync: %v\n", err)
	}
	if err := igst.Close(); err != nil {
		lg.Error("Failed to close: %v\n", err)
	}
}

func run(tag entry.EntryTag, src net.IP, wg *sync.WaitGroup, ctx context.Context) {
	for {
		cmd := exec.Command("log", "stream", "--style=json")
		out, err := cmd.StdoutPipe()
		if err != nil {
			lg.Fatal("Failed to get stdoutpipe: %v\n", err)
		}
		err = cmd.Start()
		if err != nil {
			lg.Error("Failed to start log: %v\n", err)
			time.Sleep(PERIOD)
			continue
		}
		for {
			ents, err := decode(out)
			if err != nil {
				lg.Error("Failed to decode: %v\n", err)
				break
			}

			for _, v := range ents {
				v.SRC = src
				v.TS = entry.Now()
				v.Tag = tag
			}

			if err = igst.WriteBatchContext(ctx, ents); err != nil {
				if err == context.Canceled {
					return
				}
				lg.Error("Sending message: %v", err)
			}

		}
		cmd.Process.Kill()
	}
}

var buf []byte
var first = true

func decode(r io.Reader) ([]*entry.Entry, error) {
	if first {
		b := make([]byte, 1024)
		for {
			n, err := r.Read(b)
			if err != nil {
				return nil, err
			}
			if n > 0 {
				buf = append(buf, b[:n]...)
			}
			if len(buf) >= 3 {
				// pop off the leading [{\n
				buf = buf[3:]
				first = false
				break
			}
			time.Sleep(READ_PERIOD)
		}
	}

	var ents []*entry.Entry

	for {
		b := make([]byte, 1024)
		n, err := r.Read(b)
		if err != nil {
			return nil, err
		}

		buf = append(buf, b[:n]...)

		e := bytes.Split(buf, []byte("\n},{\n"))
		if len(e) <= 1 {
			time.Sleep(READ_PERIOD)
			continue
		}

		// consume all but the last piece
		for i := 0; i < len(e)-1; i++ {
			d := []byte{'{'}
			d = append(d, e[i]...)
			d = append(d, '}')
			var o bytes.Buffer
			err := json.Compact(&o, d)
			if err != nil {
				return nil, err
			}
			ents = append(ents, &entry.Entry{
				Data: o.Bytes(),
			})
		}

		buf = e[len(e)-1]
		break
	}

	return ents, nil
}
