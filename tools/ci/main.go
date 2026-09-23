// Command ci is the local run-all: every check CI runs that this machine can,
// in CI's order, with nothing but Go (plus Docker for the optional MinIO tier).
//
//	go run ./tools/ci                 # everything
//	go run ./tools/ci -quick          # fmt, vet, lint, race
//	go run ./tools/ci -only lint,race # named steps
//
// Every step runs even after one fails, and the summary lists them all, so one
// red does not hide another.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"time"
)

type step struct {
	name  string
	quick bool
	run   func(ctx context.Context, c *config) error
}

// errSkip marks a step that could not run here. It is shown as SKIP in the
// summary, never as PASS (docs/TESTING.md §7: a skip is not a pass).
type errSkip struct{ why string }

func (e errSkip) Error() string { return e.why }

type config struct {
	base            string
	fuzzTime        string
	crashIterations int
	required        map[string]bool // steps named with -only must not skip
}

var steps = []step{
	{"fmt", true, stepFmt},
	{"vet", true, stepVet},
	{"modverify", false, stepModVerify},
	{"lint", true, stepLint},
	{"vuln", false, stepVuln},
	{"race", true, stepRace},
	{"cover", false, stepCover},
	{"redcheck", false, stepRedcheck},
	{"crash", false, stepCrash},
	{"slow", false, stepSlow},
	{"fuzz", false, stepFuzz},
	{"mutate", false, stepMutate},
	{"minio", false, stepMinio},
}

func main() { os.Exit(run()) }

func run() int {
	quick := flag.Bool("quick", false, "run only the fast steps (fmt, vet, lint, race)")
	only := flag.String("only", "", "comma-separated steps to run: "+stepNames())
	c := &config{required: map[string]bool{}}
	flag.StringVar(&c.base, "base", "", "red-check branch point (default: main, else origin/main, else the root commit)")
	flag.StringVar(&c.fuzzTime, "fuzztime", envOr("FUZZTIME", "30s"), "time per fuzz target")
	flag.IntVar(&c.crashIterations, "crash-iterations", 100, "kill -9 iterations per crash harness (nightly: 1000)")
	flag.Parse()

	var selected []step
	if *only != "" {
		want := map[string]bool{}
		for _, n := range strings.Split(*only, ",") {
			want[strings.TrimSpace(n)] = true
		}
		for _, s := range steps {
			if want[s.name] {
				selected = append(selected, s)
				c.required[s.name] = true
				delete(want, s.name)
			}
		}
		if len(want) > 0 {
			fmt.Fprintf(os.Stderr, "ci: unknown steps %v (have %s)\n", keys(want), stepNames())
			return 2
		}
	} else {
		for _, s := range steps {
			if !*quick || s.quick {
				selected = append(selected, s)
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	type result struct {
		name   string
		status string
		dur    time.Duration
		detail string
	}
	var results []result
	failed := false
	for _, s := range selected {
		fmt.Printf("\n=== ci: %s\n", s.name)
		start := time.Now()
		err := s.run(ctx, c)
		r := result{name: s.name, dur: time.Since(start).Round(time.Millisecond), status: "PASS"}
		var skip errSkip
		switch {
		case errors.As(err, &skip) && c.required[s.name]:
			r.status, r.detail, failed = "FAIL", "required but could not run: "+skip.why, true
		case errors.As(err, &skip):
			r.status, r.detail = "SKIP", skip.why
		case err != nil:
			r.status, r.detail, failed = "FAIL", err.Error(), true
		}
		results = append(results, r)
		if ctx.Err() != nil {
			break
		}
	}
	fmt.Printf("\n=== ci summary\n")
	for _, r := range results {
		fmt.Printf("  %-4s %-10s %9s  %s\n", r.status, r.name, r.dur, firstLine(r.detail))
	}
	if failed {
		return 1
	}
	return 0
}

func stepNames() string {
	var ns []string
	for _, s := range steps {
		ns = append(ns, s.name)
	}
	return strings.Join(ns, ",")
}

// --- steps ---

func stepFmt(ctx context.Context, _ *config) error {
	out, err := output(ctx, nil, "gofmt", "-l", ".")
	if err != nil {
		return err
	}
	if s := strings.TrimSpace(out); s != "" {
		return fmt.Errorf("not gofmt-clean:\n%s", s)
	}
	return nil
}

func stepVet(ctx context.Context, _ *config) error {
	for _, target := range [][2]string{{"linux", "amd64"}, {"windows", "amd64"}, {"darwin", "arm64"}} {
		fmt.Printf("--- go vet GOOS=%s GOARCH=%s\n", target[0], target[1])
		if err := stream(ctx, []string{"GOOS=" + target[0], "GOARCH=" + target[1]}, "go", "vet", "./..."); err != nil {
			return fmt.Errorf("go vet for %s/%s: %w", target[0], target[1], err)
		}
	}
	return nil
}

func stepModVerify(ctx context.Context, _ *config) error {
	if err := stream(ctx, nil, "go", "mod", "verify"); err != nil {
		return err
	}
	if err := stream(ctx, nil, "go", "mod", "tidy", "-diff"); err != nil {
		return fmt.Errorf("go.mod/go.sum are not tidy: %w", err)
	}
	return nil
}

func stepLint(ctx context.Context, _ *config) error {
	if _, err := exec.LookPath("golangci-lint"); err != nil {
		return errors.New("golangci-lint not on PATH: run `mise install` (it builds the pinned version with Go 1.27)")
	}
	return stream(ctx, nil, "golangci-lint", "run", "./...")
}

func stepVuln(ctx context.Context, _ *config) error {
	if _, err := exec.LookPath("govulncheck"); err != nil {
		return errors.New("govulncheck not on PATH: run `mise install`")
	}
	return stream(ctx, nil, "govulncheck", "./...")
}

func stepRace(ctx context.Context, _ *config) error {
	return stream(ctx, []string{"CGO_ENABLED=1"}, "go", "test", "-race", "-count=1", "-timeout", "30m", "./...")
}

func stepCover(ctx context.Context, _ *config) error {
	pkgs, err := productPackages(ctx)
	if err != nil {
		return err
	}
	if len(pkgs) == 0 {
		return errSkip{"no core/ or model/ packages yet"}
	}
	prof, err := os.CreateTemp("", "cover-*.out")
	if err != nil {
		return err
	}
	prof.Close()
	defer os.Remove(prof.Name())
	args := append([]string{"test", "-count=1", "-covermode=set",
		"-coverpkg=" + strings.Join(pkgs, ","), "-coverprofile=" + prof.Name()}, pkgs...)
	if err := stream(ctx, nil, "go", args...); err != nil {
		return fmt.Errorf("tests failed under -cover: %w", err)
	}
	b, err := os.ReadFile(prof.Name())
	if err != nil {
		return err
	}
	cs := CoverageFromProfile(string(b))
	for _, c := range cs {
		if th := Threshold(c.Pkg); th > 0 {
			fmt.Printf("  %-40s %5.1f%%  (gate %.0f%%)\n", c.Pkg, c.Percent, th)
		}
	}
	fs := GateCoverage(cs)
	if len(fs) == 0 {
		return nil
	}
	var sb strings.Builder
	for _, f := range fs {
		fmt.Fprintf(&sb, "%s %.1f%% < %.0f%%; ", f.Pkg, f.Percent, f.Threshold)
	}
	return fmt.Errorf("below the coverage gate: %s", strings.TrimSuffix(sb.String(), "; "))
}

func stepRedcheck(ctx context.Context, c *config) error {
	base := c.base
	if base == "" {
		var err error
		if base, err = defaultBase(ctx); err != nil {
			return err
		}
	}
	return stream(ctx, nil, "go", "run", "./tools/redcheck", "-base", base)
}

func stepCrash(ctx context.Context, c *config) error {
	pkgs, err := productPackages(ctx)
	if err != nil {
		return err
	}
	if len(pkgs) == 0 {
		return errSkip{"no core/ packages yet"}
	}
	args := append([]string{"test", "-count=1", "-run", "Crash", "-timeout", "60m"}, pkgs...)
	return stream(ctx, []string{fmt.Sprintf("SNAPSHOT_CRASH_ITERATIONS=%d", c.crashIterations)}, "go", args...)
}

// stepSlow runs the scale guards behind -tags slow (the spec's 1 GiB file,
// 1M-entry maps): too slow for every push, run nightly and by `mise run ci`.
func stepSlow(ctx context.Context, _ *config) error {
	pkgs, err := productPackages(ctx)
	if err != nil {
		return err
	}
	if len(pkgs) == 0 {
		return errSkip{"no core/ packages yet"}
	}
	args := append([]string{"test", "-count=1", "-tags", "slow", "-run", "Slow", "-timeout", "60m"}, pkgs...)
	return stream(ctx, nil, "go", args...)
}

var fuzzName = regexp.MustCompile(`^Fuzz\w+$`)

func stepFuzz(ctx context.Context, c *config) error {
	pkgs, err := productPackages(ctx)
	if err != nil {
		return err
	}
	type target struct{ pkg, name string }
	var targets []target
	for _, p := range pkgs {
		out, err := output(ctx, nil, "go", "test", "-list", "^Fuzz", p)
		if err != nil {
			return fmt.Errorf("listing fuzz targets in %s: %w", p, err)
		}
		for _, l := range strings.Split(out, "\n") {
			if fuzzName.MatchString(strings.TrimSpace(l)) {
				targets = append(targets, target{p, strings.TrimSpace(l)})
			}
		}
	}
	if len(targets) == 0 {
		return errSkip{"no fuzz targets yet"}
	}
	for _, t := range targets {
		fmt.Printf("--- fuzz %s %s for %s\n", t.pkg, t.name, c.fuzzTime)
		if err := stream(ctx, nil, "go", "test", "-run", "^$", "-fuzz", "^"+t.name+"$", "-fuzztime", c.fuzzTime, t.pkg); err != nil {
			return fmt.Errorf("%s in %s: %w (minimize the input and check it in under testdata/fuzz)", t.name, t.pkg, err)
		}
	}
	return nil
}

func stepMutate(ctx context.Context, _ *config) error {
	return stream(ctx, nil, "go", "run", "./tools/mutate")
}

// minioImage is the MinIO the S3 suites were proven against, pinned by
// digest. Docker Hub no longer serves minio/minio; quay.io still does.
const minioImage = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"

// stepMinio runs the S3 suites against a real MinIO. With SNAPSHOT_S3_ENDPOINT
// set it uses that server; otherwise it starts a throwaway container.
func stepMinio(ctx context.Context, _ *config) error {
	if _, err := os.Stat("core/blob/s3"); err != nil {
		return errSkip{"no S3 backend yet"}
	}
	env := []string{"SNAPSHOT_S3_REQUIRED=1"}
	if os.Getenv("SNAPSHOT_S3_ENDPOINT") == "" {
		if _, err := output(ctx, nil, "docker", "info"); err != nil {
			return errSkip{"Docker unavailable and SNAPSHOT_S3_ENDPOINT unset"}
		}
		id, err := output(ctx, nil, "docker", "run", "-d", "--rm", "-p", "127.0.0.1::9000",
			"-e", "MINIO_ROOT_USER=snapshotcore", "-e", "MINIO_ROOT_PASSWORD=snapshotcore-secret",
			minioImage, "server", "/data")
		if err != nil {
			return fmt.Errorf("starting MinIO: %w", err)
		}
		id = strings.TrimSpace(id)
		defer func() { _, _ = output(context.Background(), nil, "docker", "stop", id) }()
		port, err := output(ctx, nil, "docker", "port", id, "9000/tcp")
		if err != nil {
			return fmt.Errorf("reading MinIO's port: %w", err)
		}
		endpoint := "http://" + strings.TrimSpace(strings.Split(strings.TrimSpace(port), "\n")[0])
		if err := waitHTTP(ctx, endpoint+"/minio/health/ready", 60*time.Second); err != nil {
			return err
		}
		env = append(env, "SNAPSHOT_S3_ENDPOINT="+endpoint, "SNAPSHOT_S3_ACCESS_KEY=snapshotcore",
			"SNAPSHOT_S3_SECRET_KEY=snapshotcore-secret", "SNAPSHOT_S3_BUCKET=snapshot-core-test")
	}
	pkgs, err := productPackages(ctx)
	if err != nil {
		return err
	}
	var s3pkgs []string
	for _, p := range pkgs {
		if strings.HasPrefix(p, "./core/blob/s3") || strings.HasPrefix(p, "./core/chunk") {
			s3pkgs = append(s3pkgs, p)
		}
	}
	args := append([]string{"test", "-count=1", "-timeout", "20m"}, s3pkgs...)
	return stream(ctx, env, "go", args...)
}

// --- helpers ---

func productPackages(ctx context.Context) ([]string, error) {
	out, err := output(ctx, nil, "go", "list", "-e", "./...")
	if err != nil {
		return nil, err
	}
	var pkgs []string
	for _, p := range strings.Fields(out) {
		rel := strings.TrimPrefix(p, modulePrefix)
		if strings.HasPrefix(rel, "core/") || strings.HasPrefix(rel, "model/") {
			pkgs = append(pkgs, "./"+rel)
		}
	}
	return pkgs, nil
}

func defaultBase(ctx context.Context) (string, error) {
	for _, ref := range []string{"main", "origin/main"} {
		if _, err := output(ctx, nil, "git", "merge-base", ref, "HEAD"); err == nil {
			return ref, nil
		}
	}
	root, err := output(ctx, nil, "git", "rev-list", "--max-parents=0", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(strings.Split(strings.TrimSpace(root), "\n")[0]), nil
}

func waitHTTP(ctx context.Context, url string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("%s not ready after %s", url, limit)
}

func stream(ctx context.Context, env []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func output(ctx context.Context, env []string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	var b bytes.Buffer
	cmd.Stdout, cmd.Stderr = &b, &b
	err := cmd.Run()
	if s := strings.TrimSpace(b.String()); err != nil && s != "" {
		err = fmt.Errorf("%w: %s", err, s) // the error says why, not only "exit status 125"
	}
	return b.String(), err
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func keys(m map[string]bool) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func firstLine(s string) string {
	l, _, _ := strings.Cut(s, "\n")
	return l
}
