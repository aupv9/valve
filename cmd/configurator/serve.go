package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/aupv9/valve/internal/admin"
	"github.com/aupv9/valve/internal/metrics"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/tlsutil"
	"github.com/aupv9/valve/internal/xds"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func serve(args []string, log *slog.Logger) error {
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	policyPath := flags.String("policy", "policies", "policy file or directory (a Git checkout)")
	listen := flags.String("listen", ":18000", "xDS (ADS) gRPC listen address")
	adminAddr := flags.String("admin", ":8080", "admin HTTP address (kill switch)")
	adminToken := flags.String("admin-token", os.Getenv("VALVE_ADMIN_TOKEN"), "bearer token required to change the kill switch (empty = no auth)")
	poll := flags.Duration("poll", 5*time.Second, "how often to re-read policies")
	state := flags.String("state", "", "file that persists the kill switch across restarts (empty = in memory)")
	tlsCfg := tlsutil.RegisterFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	creds, err := tlsCfg.ServerCredentials()
	if err != nil {
		return err
	}
	reg := admin.NewRegistry()
	m := metrics.NewConfigurator(reg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := xds.NewServer(ctx)
	ks := &killSwitch{srv: srv, path: *state, log: log, metrics: m}
	if err := ks.load(); err != nil {
		return err
	}
	w := &watcher{srv: srv, paths: []string{*policyPath}, log: log, metrics: m}
	if err := w.reload(); err != nil {
		return fmt.Errorf("initial policy load: %w", err)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	g := grpc.NewServer(append(xds.ServerOptions(), grpc.Creds(creds))...)
	srv.Register(g)

	mux := admin.Mux(reg, nil)
	mux.HandleFunc("GET /kill-switch", ks.get)
	mux.Handle("PUT /kill-switch", requireToken(*adminToken, http.HandlerFunc(ks.put)))
	hs := &http.Server{Addr: *adminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go w.run(ctx, *poll)
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server", "err", err)
			stop()
		}
	}()
	go func() {
		<-ctx.Done()
		g.GracefulStop()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hs.Shutdown(sctx) //nolint:errcheck // best effort on exit
	}()

	log.Info("configurator serving", "xds", lis.Addr().String(), "admin", *adminAddr, "policy", *policyPath,
		"admin_auth", *adminToken != "", "mtls", tlsCfg.Enabled())
	return g.Serve(lis)
}

type watcher struct {
	srv     *xds.Server
	paths   []string
	log     *slog.Logger
	metrics *metrics.Configurator
	version string
}

func (w *watcher) reload() error {
	pf, err := policy.LoadPaths(w.paths...)
	if err != nil {
		w.metrics.ReloadErrors.Inc()
		return err
	}
	ps := pf.ToProto()
	if ps.GetVersion() == w.version {
		return nil
	}
	if err := w.srv.SetPolicies(ps); err != nil {
		return err
	}
	w.log.Info("published policies", "version", ps.GetVersion(), "buckets", len(ps.GetBuckets()), "previous", w.version)
	w.metrics.Published.Inc()
	w.metrics.SetConfigVersion(policy.Version(ps))
	w.version = ps.GetVersion()
	return nil
}

func (w *watcher) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := w.reload(); err != nil {
				w.log.Error("policy change rejected, keeping last valid version", "version", w.version, "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

type killSwitch struct {
	srv     *xds.Server
	path    string
	log     *slog.Logger
	metrics *metrics.Configurator

	mu  sync.Mutex
	cur *rlv1.KillSwitch
}

type killSwitchJSON struct {
	Enabled   bool      `json:"enabled"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitzero"`
}

func (k *killSwitch) load() error {
	k.cur = &rlv1.KillSwitch{}
	if k.path == "" {
		return nil
	}
	b, err := os.ReadFile(k.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var j killSwitchJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return fmt.Errorf("kill switch state %s: %w", k.path, err)
	}
	return k.set(j, false)
}

func (k *killSwitch) set(j killSwitchJSON, persist bool) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	ks := &rlv1.KillSwitch{Enabled: j.Enabled, Reason: j.Reason, UpdatedUnixMs: j.UpdatedAt.UnixMilli()}
	if persist && k.path != "" {
		b, _ := json.MarshalIndent(j, "", "  ")
		tmp := k.path + ".tmp"
		if err := os.MkdirAll(filepath.Dir(k.path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(tmp, b, 0o644); err != nil {
			return err
		}
		if err := os.Rename(tmp, k.path); err != nil {
			return err
		}
	}
	if err := k.srv.SetKillSwitch(ks); err != nil {
		return err
	}
	k.cur = ks
	k.metrics.KillSwitch.Set(metrics.Bool(ks.GetEnabled()))
	k.log.Warn("kill switch", "enabled", j.Enabled, "reason", j.Reason)
	return nil
}

func (k *killSwitch) get(w http.ResponseWriter, _ *http.Request) {
	k.mu.Lock()
	cur := k.cur
	k.mu.Unlock()
	writeJSON(w, http.StatusOK, killSwitchJSON{
		Enabled: cur.GetEnabled(), Reason: cur.GetReason(), UpdatedAt: time.UnixMilli(cur.GetUpdatedUnixMs()).UTC(),
	})
}

func (k *killSwitch) put(w http.ResponseWriter, r *http.Request) {
	var j killSwitchJSON
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&j); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	j.UpdatedAt = time.Now().UTC()
	if err := k.set(j, true); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func requireToken(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck // client went away
}
