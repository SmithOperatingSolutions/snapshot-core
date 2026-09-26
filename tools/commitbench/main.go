// Command commitbench measures what a commit costs, the way a host commits:
// on blob/local (fsync, default options) and blob/mem, through the version
// graph's one-publish Commit (repo.Repo embeds *vcs.Repo, and Commit is
// that method).
//
//	go run ./tools/commitbench                      # everything, both backends
//	go run ./tools/commitbench -backends local -only single -cpuprofile cpu.prof
//	go run ./tools/commitbench -backends mem -only batch -batch 10000 -cpuprofile cpu.prof -memprofile mem.prof
//
// The stack is the one repo.Open builds (repo.Init writes the config and the
// first root; packstore.Open on blob.NoDelete of the store, then vcs.Open),
// assembled here from the same exported pieces so that two wrappers can
// watch it: one times the backend's calls by kind, one counts the root swaps
// a writer lost. Runs:
//
// Each single-writer and concurrent run lasts -duration (20s), and the bulk
// run is sized to finish well inside a minute: no run takes longer.
//
//   - single: one writer, one small object per commit, back to back:
//     commits/s, p50 and p99, and where a commit's time goes;
//   - concurrent: 4, 16 and 64 writers, each on a branch of its own, so no
//     working set conflicts, but every commit swaps the one root: commits/s,
//     p99, and the swaps lost to another writer (each is retried by vcs);
//   - bulk: a 256 MiB random stream written, then one commit;
//   - batch: 1,000 and 10,000 small objects written into one branch's
//     namespace through one editor, flushed once, committed once: writes/s,
//     the flush, the commit and the publish inside it, and the pack bytes
//     the commit wrote; -reader plain hides each object's length from the
//     write, -reader hinted hints it with stream.WithLen (#41);
//   - read: point reads of 10,000 small objects committed once (1 and 16
//     readers, -readers: Gets/s, p50, p99), a full read of the bulk stream
//     (MB/s), and the 16 readers again while one writer commits back to back
//     (through the journal where the backend keeps one): what a background
//     publish costs readers.
//
// -journal commits through the backend's journal (#34), and the tables add
// the journal's appends.
//
// Timed runs on a shared machine take the measurement lock and start with
// the load under 2 (CONTRIBUTING.md, "Heavy runs"); the tool prints the load
// it started under beside the figures.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	mblob "github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

func main() { os.Exit(run()) }

type config struct {
	backends   []string
	only       map[string]bool
	dir        string
	writers    []int
	batches    []int
	duration   time.Duration
	bulkMiB    int
	cpuprofile string
	memprofile string
	objectSize int
	readers    []int
	reader     string
	flow       string
	journal    string
	interval   time.Duration
}

// objectReader is how the batch run hands a small object to the write:
// "lener" a strings.Reader, which reports its length (cdc.Lener); "plain"
// the same reader hiding its length, as a network stream or a pipe does;
// "hinted" that plain reader with its length hinted by stream.WithLen (#41).
func (c *config) objectReader(body string) io.Reader {
	switch c.reader {
	case "plain":
		return struct{ io.Reader }{strings.NewReader(body)}
	case "hinted":
		return stream.WithLen(struct{ io.Reader }{strings.NewReader(body)}, len(body))
	default:
		return strings.NewReader(body)
	}
}

func run() int {
	var c config
	backends := flag.String("backends", "local,mem", "comma-separated backends: local, mem")
	only := flag.String("only", "single,concurrent,bulk,batch", "comma-separated runs: single, concurrent, bulk, batch, read")
	readers := flag.String("readers", "1,16", "comma-separated reader counts for the read run's point reads")
	batches := flag.String("batch", "1000,10000", "comma-separated object counts for the batch run")
	writers := flag.String("writers", "4,16,64", "comma-separated writer counts for the concurrent run")
	flag.StringVar(&c.dir, "dir", os.TempDir(), "where blob/local stores are created (and removed afterwards)")
	flag.DurationVar(&c.duration, "duration", 20*time.Second, "length of each single-writer and concurrent run (the owner's bound: a run takes under a minute)")
	flag.IntVar(&c.bulkMiB, "bulk", 256, "MiB written before the bulk run's commit")
	flag.StringVar(&c.cpuprofile, "cpuprofile", "", "write a CPU profile of the single-writer run on the first backend here (without -only single: of the first batch run's write phase)")
	flag.StringVar(&c.memprofile, "memprofile", "", "write an allocation profile on the first backend here: of the first batch run's write phase with -only batch, else of the single-writer run with -cpuprofile")
	flag.IntVar(&c.objectSize, "object", 100, "bytes in each small object")
	flag.StringVar(&c.reader, "reader", "lener", "how the batch run hands each object to the write: lener (a strings.Reader), plain (a reader without a length), hinted (plain, with stream.WithLen)")
	flag.StringVar(&c.flow, "flow", "commit", "how a commit is made: commit (CommitNamespace, one publish), two-step (UpdateWorkingSet then CommitWorkingSet), two-step-flushed (their Flushed forms, handed the flush's record)")
	flag.StringVar(&c.journal, "journal", "default", "commit through the backend's journal: on, off or default (packstore.Options.Journal, #34)")
	flag.DurationVar(&c.interval, "interval", 0, "the journal's publish interval (0: packstore.DefaultJournalInterval)")
	flag.Parse()
	if c.reader != "lener" && c.reader != "plain" && c.reader != "hinted" {
		fmt.Fprintf(os.Stderr, "commitbench: bad -reader %q\n", c.reader)
		return 2
	}
	if c.memprofile != "" {
		// Set once, before anything allocates, and finer than the default:
		// the objects are small. The profile counts from here, so it holds
		// the setup too, a small share of a batch's allocations.
		runtime.MemProfileRate = 4096
	}
	c.backends = strings.Split(*backends, ",")
	c.only = map[string]bool{}
	for _, o := range strings.Split(*only, ",") {
		c.only[strings.TrimSpace(o)] = true
	}
	for _, b := range strings.Split(*batches, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(b))
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "commitbench: bad batch size %q\n", b)
			return 2
		}
		c.batches = append(c.batches, n)
	}
	for _, w := range strings.Split(*writers, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(w))
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "commitbench: bad writer count %q\n", w)
			return 2
		}
		c.writers = append(c.writers, n)
	}
	for _, w := range strings.Split(*readers, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(w))
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "commitbench: bad reader count %q\n", w)
			return 2
		}
		c.readers = append(c.readers, n)
	}
	if err := bench(c); err != nil {
		fmt.Fprintln(os.Stderr, "commitbench:", err)
		return 1
	}
	return 0
}

func loadavg() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "unknown"
	}
	return strings.Join(strings.Fields(string(b))[:3], " ")
}

func bench(c config) error {
	host, _ := os.Hostname()
	fmt.Printf("commitbench: %s, load %s, %s, journal %s (interval %v)\n", host, loadavg(), time.Now().Format(time.RFC3339), c.journal, c.interval)
	if slices.Contains(c.backends, "local") {
		floor, err := fsyncFloor(c.dir)
		if err != nil {
			return err
		}
		fmt.Printf("fsync floor on %s: write 4 KiB + fsync, p50 %v p99 %v; with a link and a directory fsync (a blob/local Put) p50 %v\n",
			c.dir, floor.p50, floor.p99, floor.linkP50)
	}
	var rows []row
	var breakdowns []string
	var batchRows []string
	var readRows []row
	for i, b := range c.backends {
		if c.only["single"] {
			prof := ""
			if i == 0 {
				prof = c.cpuprofile
			}
			r, bd, err := single(c, b, prof)
			if err != nil {
				return fmt.Errorf("%s single: %w", b, err)
			}
			rows = append(rows, r)
			breakdowns = append(breakdowns, bd)
		}
		if c.only["concurrent"] {
			for _, w := range c.writers {
				r, err := concurrent(c, b, w)
				if err != nil {
					return fmt.Errorf("%s %d writers: %w", b, w, err)
				}
				rows = append(rows, r)
			}
		}
		if c.only["batch"] {
			for j, n := range c.batches {
				var prof profiles
				if i == 0 && j == 0 {
					prof.mem = c.memprofile
					if !c.only["single"] {
						prof.cpu = c.cpuprofile
					}
				}
				r, err := batch(c, b, n, prof)
				if err != nil {
					return fmt.Errorf("%s batch of %d: %w", b, n, err)
				}
				batchRows = append(batchRows, r)
			}
		}
		if c.only["read"] {
			rs, err := reads(c, b)
			if err != nil {
				return fmt.Errorf("%s read: %w", b, err)
			}
			readRows = append(readRows, rs...)
		}
		if c.only["bulk"] {
			r, err := bulk(c, b)
			if err != nil {
				return fmt.Errorf("%s bulk: %w", b, err)
			}
			rows = append(rows, r)
		}
	}
	if len(rows) > 0 {
		fmt.Printf("\n| backend | run | writers | commits | commits/s | p50 | p99 | lost swaps |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
		for _, r := range rows {
			fmt.Println(r)
		}
	}
	if len(batchRows) > 0 {
		fmt.Printf("\n| backend | objects | write phase | writes/s | flush | commit | publish in it | total | pack bytes the commit wrote | journal bytes it appended |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
		for _, r := range batchRows {
			fmt.Println(r)
		}
	}
	if len(readRows) > 0 {
		fmt.Printf("\n| backend | run | readers | reads | reads/s | p50 | p99 | commits beside |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
		for _, r := range readRows {
			fmt.Println(r)
		}
	}
	for _, bd := range breakdowns {
		fmt.Print("\n" + bd)
	}
	fmt.Printf("\nload at the end %s\n", loadavg())
	return nil
}

type row struct {
	backend, run   string
	writers, count int
	rate           float64
	p50, p99       time.Duration
	lost           int64
	note           string
}

func (r row) String() string {
	s := fmt.Sprintf("| %s | %s | %d | %d | %.1f | %v | %v | %d |", r.backend, r.run, r.writers, r.count, r.rate,
		r.p50.Round(10*time.Microsecond), r.p99.Round(10*time.Microsecond), r.lost)
	if r.note != "" {
		s += " " + r.note
	}
	return s
}

// --- the stack ---

type env struct {
	backend string
	dir     string
	raw     blob.BlobStore
	timed   *timedBlobs
	chunks  *packstore.Store
	counted *countingStore
	v       *vcs.Repo
	geo     repo.Geometry
	me      auth.Principal
	flow    string
}

func newEnv(c config, backend string) (*env, error) {
	ctx := context.Background()
	e := &env{backend: backend, me: auth.Principal{ID: "user:bench"}, flow: c.flow}
	switch backend {
	case "local":
		dir, err := os.MkdirTemp(c.dir, "commitbench-")
		if err != nil {
			return nil, err
		}
		e.dir = dir
		bs, err := local.Create(filepath.Join(dir, "store"), local.Options{})
		if err != nil {
			return nil, err
		}
		e.raw = bs
	case "mem":
		e.raw = mem.New()
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
	keys, err := seal.NewKeyring()
	if err != nil {
		return nil, err
	}
	e.geo = repo.DefaultGeometry()
	models, err := model.NewRegistry(mblob.Model{}, tree.Model{Config: e.geo.Prolly()})
	if err != nil {
		return nil, err
	}
	o := repo.Options{Blobs: e.raw, Keys: keys, Registry: models, Authorizer: auth.AllowAll{}}
	r, err := repo.Init(ctx, e.me, o)
	if err != nil {
		return nil, err
	}
	cfg := r.Config
	if err := r.Close(); err != nil {
		return nil, err
	}
	// repo.Open's stack, with the two wrappers.
	e.timed = &timedBlobs{BlobStore: e.raw}
	e.chunks, err = packstore.Open(ctx, packstore.Options{Blobs: blob.NoDelete(e.timed), Keys: keys, Repo: cfg.RepoID, PackSize: cfg.Geometry.PackSize,
		Journal: journalMode(c.journal), JournalInterval: c.interval})
	if err != nil {
		return nil, err
	}
	e.counted = &countingStore{Store: e.chunks}
	e.v, err = vcs.Open(ctx, e.counted, vcs.Options{Config: cfg.Geometry.Prolly(), Registry: models, Authorizer: auth.AllowAll{}})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *env) close() {
	_ = e.chunks.Close()
	if e.dir != "" {
		_ = os.RemoveAll(e.dir)
	}
}

// commitOne writes one small object at path on branch and commits it.
func (e *env) commitOne(ctx context.Context, branch, path string, size int) error {
	body := make([]byte, size)
	_, _ = rand.Read(body)
	root, err := mblob.Write(ctx, e.chunks, strings.NewReader(string(body)), e.geo.Stream())
	if err != nil {
		return err
	}
	return e.commitRef(ctx, branch, path, root)
}

func (e *env) commitRef(ctx context.Context, branch, path string, root model.Root) error {
	return e.commitVia(ctx, e.flow, branch, path, root)
}

// commitVia is commitRef by one of -flow's ways.
func (e *env) commitVia(ctx context.Context, flow, branch, path string, root model.Root) error {
	ws, err := e.v.WorkingSet(ctx, e.me, branch)
	if err != nil {
		return err
	}
	n, err := e.v.Namespace(ctx, ws.Working)
	if err != nil {
		return err
	}
	ed := n.Editor()
	if err := ed.Put(path, object.Ref{Model: mblob.ID, Root: root}); err != nil {
		return err
	}
	if n, err = ed.Flush(ctx); err != nil {
		return err
	}
	switch flow {
	case "two-step", "two-step-flushed":
		next := ws
		next.Working, next.Staged = n.Root(), n.Root()
		var rec []*object.Namespace
		if flow == "two-step-flushed" {
			rec = []*object.Namespace{n}
		}
		if _, err := e.v.UpdateWorkingSetFlushed(ctx, e.me, branch, ws, next, rec...); err != nil {
			return err
		}
		_, err = e.v.CommitWorkingSetFlushed(ctx, e.me, branch, "put "+path, rec...)
	default:
		_, err = e.v.CommitNamespace(ctx, e.me, branch, ws, n, "put "+path)
	}
	return err
}

// timedBlobs times the backend's calls by kind.
type timedBlobs struct {
	blob.BlobStore
	packPut, indexPut, otherPut, swap, root, get, appends stat
	packBytes, appendBytes                                atomic.Int64
}

// The timed store keeps its backend's journal, and times its appends.
func (t *timedBlobs) OpenJournal(ctx context.Context) (blob.Journal, error) {
	js, ok := t.BlobStore.(blob.Journaler)
	if !ok {
		return nil, errors.New("commitbench: the backend keeps no journal")
	}
	j, err := js.OpenJournal(ctx)
	if err != nil {
		return nil, err
	}
	return timedJournal{j, t}, nil
}

func (t *timedBlobs) JournalByDefault() bool {
	js, ok := t.BlobStore.(blob.Journaler)
	return ok && js.JournalByDefault()
}

func (t *timedBlobs) HoldJournal(ctx context.Context) (int64, func(), error) {
	js, ok := t.BlobStore.(blob.Journaler)
	if !ok {
		return 0, func() {}, nil
	}
	return js.HoldJournal(ctx)
}

type timedJournal struct {
	blob.Journal
	t *timedBlobs
}

func (j timedJournal) Append(ctx context.Context, b []byte) error {
	t0 := time.Now()
	err := j.Journal.Append(ctx, b)
	j.t.appends.add(t0)
	if err == nil {
		j.t.appendBytes.Add(int64(len(b)))
	}
	return err
}

type stat struct{ n, ns atomic.Int64 }

func (s *stat) add(t0 time.Time) {
	s.n.Add(1)
	s.ns.Add(int64(time.Since(t0)))
}

func (s *stat) reset() { s.n.Store(0); s.ns.Store(0) }

func (t *timedBlobs) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	t0 := time.Now()
	err := t.BlobStore.Put(ctx, name, r, size)
	switch {
	case strings.HasPrefix(name, "packs/"):
		t.packPut.add(t0)
		if err == nil {
			t.packBytes.Add(size)
		}
	case strings.HasPrefix(name, "index/"):
		t.indexPut.add(t0)
	default:
		t.otherPut.add(t0)
	}
	return err
}

func (t *timedBlobs) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	t0 := time.Now()
	rc, err := t.BlobStore.Get(ctx, name, off, n)
	t.get.add(t0)
	return rc, err
}

func (t *timedBlobs) Root(ctx context.Context) (blob.Root, error) {
	t0 := time.Now()
	r, err := t.BlobStore.Root(ctx)
	t.root.add(t0)
	return r, err
}

func (t *timedBlobs) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	t0 := time.Now()
	v, err := t.BlobStore.SwapRoot(ctx, expected, next)
	t.swap.add(t0)
	return v, err
}

func (t *timedBlobs) reset() {
	for _, s := range []*stat{&t.packPut, &t.indexPut, &t.otherPut, &t.swap, &t.root, &t.get, &t.appends} {
		s.reset()
	}
}

// countingStore counts the chunk store's publishes, and the swaps lost to
// another writer, and times the publishes.
type countingStore struct {
	chunk.Store
	cas  stat
	lost atomic.Int64
}

// PutRaw passes the tree's raw hint (chunk.RawWriter, #42) through to the
// store, as repo.Open's stack, which hands vcs the store itself, does.
func (c *countingStore) PutRaw(ctx context.Context, data []byte) (hash.Hash, error) {
	if rw, ok := c.Store.(chunk.RawWriter); ok {
		return rw.PutRaw(ctx, data)
	}
	return c.Put(ctx, data)
}

func (c *countingStore) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	t0 := time.Now()
	err := c.Store.CompareAndSetRoot(ctx, expected, next)
	c.cas.add(t0)
	if errors.Is(err, chunk.ErrRootConflict) {
		c.lost.Add(1)
	}
	return err
}

// --- runs ---

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	i := int(p * float64(len(s)-1))
	return s[i]
}

func single(c config, backend, profile string) (row, string, error) {
	ctx := context.Background()
	e, err := newEnv(c, backend)
	if err != nil {
		return row{}, "", err
	}
	defer e.close()
	// A few commits first, so the run measures the steady state.
	for i := range 10 {
		if err := e.commitOne(ctx, vcs.MainBranch, fmt.Sprintf("warm/%d", i), c.objectSize); err != nil {
			return row{}, "", err
		}
	}
	e.timed.reset()
	e.counted.cas.reset()
	e.counted.lost.Store(0)
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			return row{}, "", err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return row{}, "", err
		}
		defer pprof.StopCPUProfile()
	}
	var lat []time.Duration
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t0 := time.Now()
	deadline := t0.Add(c.duration)
	for i := 0; time.Now().Before(deadline); i++ {
		s := time.Now()
		if err := e.commitOne(ctx, vcs.MainBranch, fmt.Sprintf("obj/%06d", i), c.objectSize); err != nil {
			return row{}, "", err
		}
		lat = append(lat, time.Since(s))
	}
	el := time.Since(t0)
	runtime.ReadMemStats(&m1)
	if profile != "" {
		pprof.StopCPUProfile()
		if c.memprofile != "" && !c.only["batch"] {
			if err := writeAllocs(c.memprofile); err != nil {
				return row{}, "", err
			}
		}
	}
	r := row{backend: backend, run: "one writer", writers: 1, count: len(lat), rate: float64(len(lat)) / el.Seconds(),
		p50: percentile(lat, .5), p99: percentile(lat, .99), lost: e.counted.lost.Load()}
	n := uint64(len(lat))
	bd := breakdown(e, len(lat), el) + fmt.Sprintf("| allocated, per commit | %d KiB | %d objects |\n", (m1.TotalAlloc-m0.TotalAlloc)/n>>10, (m1.Mallocs-m0.Mallocs)/n)
	return r, bd, nil
}

// writeAllocs writes the allocation profile to path.
func writeAllocs(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// breakdown is where a commit's time went, per commit.
func breakdown(e *env, n int, el time.Duration) string {
	per := func(s *stat) time.Duration { return time.Duration(s.ns.Load() / int64(n)) }
	cnt := func(s *stat) float64 { return float64(s.n.Load()) / float64(n) }
	t := e.timed
	cas := per(&e.counted.cas)
	pack, index, swap, root := per(&t.packPut), per(&t.indexPut), per(&t.swap), per(&t.root)
	// The packs and the index objects are written at once: the publish
	// waits for the longer.
	rest := cas - per(&t.appends) - max(pack, index) - swap - root
	var b strings.Builder
	fmt.Fprintf(&b, "%s, one writer, per commit (mean of %d; %v in all; with the journal the pack, index and swap rows are the background publishes, spread over the commits):\n", e.backend, n, el.Round(time.Millisecond))
	fmt.Fprintf(&b, "| part | time | calls |\n| --- | --- | --- |\n")
	fmt.Fprintf(&b, "| commit, end to end | %v | 1 |\n", (el / time.Duration(n)).Round(time.Microsecond))
	fmt.Fprintf(&b, "| host work outside the publish (object write, namespace edit, refs edit, reads) | %v | |\n", (el/time.Duration(n) - cas).Round(time.Microsecond))
	fmt.Fprintf(&b, "| publish (CompareAndSetRoot) | %v | %.2f |\n", cas.Round(time.Microsecond), cnt(&e.counted.cas))
	fmt.Fprintf(&b, "| . journal append (write + fsync), in the commit | %v | %.2f |\n", per(&t.appends).Round(time.Microsecond), cnt(&t.appends))
	fmt.Fprintf(&b, "| . pack finish+upload+fsync (Put packs/) | %v | %.2f |\n", pack.Round(time.Microsecond), cnt(&t.packPut))
	fmt.Fprintf(&b, "| . index object (Put index/, beside the pack) | %v | %.2f |\n", index.Round(time.Microsecond), cnt(&t.indexPut))
	fmt.Fprintf(&b, "| . root swap (SwapRoot: temp, fsync, rename, dir fsync) | %v | %.2f |\n", swap.Round(time.Microsecond), cnt(&t.swap))
	fmt.Fprintf(&b, "| . root reads (Root) | %v | %.2f |\n", root.Round(time.Microsecond), cnt(&t.root))
	fmt.Fprintf(&b, "| . manifest and the rest (seal, compaction, pack build, waits) | %v | |\n", rest.Round(time.Microsecond))
	fmt.Fprintf(&b, "| reads from the backend (Get) | %v | %.2f |\n", per(&t.get).Round(time.Microsecond), cnt(&t.get))
	return b.String()
}

func concurrent(c config, backend string, writers int) (row, error) {
	ctx := context.Background()
	e, err := newEnv(c, backend)
	if err != nil {
		return row{}, err
	}
	defer e.close()
	head, err := e.v.Head(ctx, e.me, vcs.MainBranch)
	if err != nil {
		return row{}, err
	}
	for w := range writers {
		if err := e.v.CreateBranch(ctx, e.me, fmt.Sprintf("w%02d", w), head.Hash); err != nil {
			return row{}, err
		}
	}
	e.counted.lost.Store(0)
	var (
		mu    sync.Mutex
		lat   []time.Duration
		first error
		wg    sync.WaitGroup
	)
	deadline := time.Now().Add(c.duration)
	t0 := time.Now()
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			branch := fmt.Sprintf("w%02d", w)
			var mine []time.Duration
			for i := 0; time.Now().Before(deadline); i++ {
				s := time.Now()
				if err := e.commitOne(ctx, branch, fmt.Sprintf("%s/%06d", branch, i), c.objectSize); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					return
				}
				mine = append(mine, time.Since(s))
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	el := time.Since(t0)
	if first != nil {
		return row{}, first
	}
	return row{backend: backend, run: "concurrent", writers: writers, count: len(lat), rate: float64(len(lat)) / el.Seconds(),
		p50: percentile(lat, .5), p99: percentile(lat, .99), lost: e.counted.lost.Load()}, nil
}

func bulk(c config, backend string) (row, error) {
	ctx := context.Background()
	e, err := newEnv(c, backend)
	if err != nil {
		return row{}, err
	}
	defer e.close()
	size := int64(c.bulkMiB) << 20
	src := make([]byte, size) // generated first: the source must not be what is measured
	cc := mrand.NewChaCha8([32]byte{7})
	_, _ = cc.Read(src)
	t0 := time.Now()
	root, err := mblob.Write(ctx, e.chunks, strings.NewReader(string(src)), e.geo.Stream())
	if err != nil {
		return row{}, err
	}
	w := time.Since(t0)
	t1 := time.Now()
	if err := e.commitRef(ctx, vcs.MainBranch, "bulk.bin", root); err != nil {
		return row{}, err
	}
	cm := time.Since(t1)
	return row{backend: backend, run: fmt.Sprintf("bulk %d MiB then commit", c.bulkMiB), writers: 1, count: 1, rate: 1 / cm.Seconds(),
		p50: cm, p99: cm, note: fmt.Sprintf("(write %v, %.0f MB/s)", w.Round(time.Millisecond), float64(size)/1e6/w.Seconds())}, nil
}

// profiles names where a batch run writes its profiles of the write phase
// ("": none).
type profiles struct{ cpu, mem string }

// start begins the profiles; the function it returns ends them and writes
// the allocation profile.
func (p profiles) start() (func() error, error) {
	var cpu *os.File
	if p.cpu != "" {
		f, err := os.Create(p.cpu)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		cpu = f
	}
	return func() error {
		if cpu != nil {
			pprof.StopCPUProfile()
			if err := cpu.Close(); err != nil {
				return err
			}
		}
		if p.mem == "" {
			return nil
		}
		return writeAllocs(p.mem)
	}, nil
}

// batch writes n small objects into one branch's namespace through one
// editor, flushes it once, and commits once: the host that batches.
func batch(c config, backend string, n int, prof profiles) (string, error) {
	ctx := context.Background()
	e, err := newEnv(c, backend)
	if err != nil {
		return "", err
	}
	defer e.close()
	if err := e.commitOne(ctx, vcs.MainBranch, "warm", c.objectSize); err != nil {
		return "", err
	}
	ws, err := e.v.WorkingSet(ctx, e.me, vcs.MainBranch)
	if err != nil {
		return "", err
	}
	ns, err := e.v.Namespace(ctx, ws.Working)
	if err != nil {
		return "", err
	}
	ed := ns.Editor()
	body := make([]byte, c.objectSize)
	stop, err := prof.start()
	if err != nil {
		return "", err
	}
	t0 := time.Now()
	for i := range n {
		_, _ = rand.Read(body)
		root, err := mblob.Write(ctx, e.chunks, c.objectReader(string(body)), e.geo.Stream())
		if err != nil {
			return "", err
		}
		if err := ed.Put(fmt.Sprintf("batch/%06d", i), object.Ref{Model: mblob.ID, Root: root}); err != nil {
			return "", err
		}
	}
	write := time.Since(t0)
	if err := stop(); err != nil {
		return "", err
	}
	t1 := time.Now()
	if ns, err = ed.Flush(ctx); err != nil {
		return "", err
	}
	flush := time.Since(t1)
	e.timed.packBytes.Store(0)
	e.timed.appendBytes.Store(0)
	e.counted.cas.reset()
	t2 := time.Now()
	if _, err := e.v.Commit(ctx, e.me, vcs.MainBranch, ws, ns.Root(), "batch"); err != nil {
		return "", err
	}
	cm := time.Since(t2)
	pub := time.Duration(e.counted.cas.ns.Load())
	r := func(d time.Duration) time.Duration { return d.Round(10 * time.Microsecond) }
	return fmt.Sprintf("| %s | %d | %v | %.0f | %v | %v | %v | %v | %d | %d |", backend, n, r(write), float64(n)/write.Seconds(),
		r(flush), r(cm), r(pub), r(write+flush+cm), e.timed.packBytes.Load(), e.timed.appendBytes.Load()), nil
}

// --- reads ---

// reads commits 10,000 small objects in one commit and reads them back by
// path from a namespace opened once: one reader and c.readers at once, then
// the most readers again beside one writer committing back to back; then
// writes the bulk stream, commits it, and reads it through.
func reads(c config, backend string) ([]row, error) {
	ctx := context.Background()
	e, err := newEnv(c, backend)
	if err != nil {
		return nil, err
	}
	defer e.close()
	const objects = 10000
	if err := e.commitOne(ctx, vcs.MainBranch, "warm", c.objectSize); err != nil {
		return nil, err
	}
	ws, err := e.v.WorkingSet(ctx, e.me, vcs.MainBranch)
	if err != nil {
		return nil, err
	}
	ns, err := e.v.Namespace(ctx, ws.Working)
	if err != nil {
		return nil, err
	}
	ed := ns.Editor()
	body := make([]byte, c.objectSize)
	for i := range objects {
		_, _ = rand.Read(body)
		root, err := mblob.Write(ctx, e.chunks, strings.NewReader(string(body)), e.geo.Stream())
		if err != nil {
			return nil, err
		}
		if err := ed.Put(fmt.Sprintf("r/%06d", i), object.Ref{Model: mblob.ID, Root: root}); err != nil {
			return nil, err
		}
	}
	if ns, err = ed.Flush(ctx); err != nil {
		return nil, err
	}
	if _, err := e.v.Commit(ctx, e.me, vcs.MainBranch, ws, ns.Root(), "objects"); err != nil {
		return nil, err
	}
	// The readers read the committed namespace, opened once, as a host
	// serving reads from a branch head would.
	ws, err = e.v.WorkingSet(ctx, e.me, vcs.MainBranch)
	if err != nil {
		return nil, err
	}
	if ns, err = e.v.Namespace(ctx, ws.Working); err != nil {
		return nil, err
	}
	get := func(path string) error {
		ref, _, ok, err := ns.Get(ctx, path)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%s not found", path)
		}
		sr, err := mblob.Open(ctx, e.chunks, ref.Root)
		if err != nil {
			return err
		}
		n, err := io.Copy(io.Discard, sr)
		if err == nil && n != int64(c.objectSize) {
			err = fmt.Errorf("%s read %d bytes, want %d", path, n, c.objectSize)
		}
		return err
	}
	var out []row
	run := func(readers int, label string, beside func(stop <-chan struct{}) (int, error)) error {
		d := c.duration
		lats := make([][]time.Duration, readers)
		errs := make([]error, readers)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		commits, werr := 0, error(nil)
		wdone := make(chan struct{})
		if beside != nil {
			go func() { commits, werr = beside(stop); close(wdone) }()
		}
		t0 := time.Now()
		deadline := t0.Add(d)
		for w := range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := mrand.New(mrand.NewPCG(uint64(w), 1))
				for time.Now().Before(deadline) {
					s := time.Now()
					if err := get(fmt.Sprintf("r/%06d", rng.IntN(objects))); err != nil {
						errs[w] = err
						return
					}
					lats[w] = append(lats[w], time.Since(s))
				}
			}()
		}
		wg.Wait()
		el := time.Since(t0)
		close(stop)
		if beside != nil {
			<-wdone
			if werr != nil {
				return werr
			}
		}
		var all []time.Duration
		for w := range readers {
			if errs[w] != nil {
				return errs[w]
			}
			all = append(all, lats[w]...)
		}
		r := row{backend: backend, run: label, writers: readers, count: len(all), rate: float64(len(all)) / el.Seconds(),
			p50: percentile(all, .5), p99: percentile(all, .99), lost: int64(commits)}
		out = append(out, r)
		return nil
	}
	most := 0
	for _, n := range c.readers {
		most = max(most, n)
		if err := run(n, "point reads", nil); err != nil {
			return nil, err
		}
	}
	writer := func(stop <-chan struct{}) (int, error) {
		n := 0
		for {
			select {
			case <-stop:
				return n, nil
			default:
			}
			if err := e.commitOne(ctx, "w", fmt.Sprintf("w/%06d", n), c.objectSize); err != nil {
				return n, err
			}
			n++
		}
	}
	head, err := e.v.Head(ctx, e.me, vcs.MainBranch)
	if err != nil {
		return nil, err
	}
	if err := e.v.CreateBranch(ctx, e.me, "w", head.Hash); err != nil {
		return nil, err
	}
	if err := run(most, "point reads, one writer committing", writer); err != nil {
		return nil, err
	}

	// The bulk stream, read through.
	size := int64(c.bulkMiB) << 20
	src := make([]byte, size)
	cc := mrand.NewChaCha8([32]byte{7})
	_, _ = cc.Read(src)
	root, err := mblob.Write(ctx, e.chunks, strings.NewReader(string(src)), e.geo.Stream())
	if err != nil {
		return nil, err
	}
	if err := e.commitRef(ctx, vcs.MainBranch, "bulk.bin", root); err != nil {
		return nil, err
	}
	t0 := time.Now()
	sr, err := mblob.Open(ctx, e.chunks, root)
	if err != nil {
		return nil, err
	}
	n, err := io.Copy(io.Discard, sr)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, fmt.Errorf("the bulk stream read %d bytes, want %d", n, size)
	}
	rd := time.Since(t0)
	out = append(out, row{backend: backend, run: fmt.Sprintf("full read of %d MiB", c.bulkMiB), writers: 1, count: 1, rate: 1 / rd.Seconds(),
		p50: rd, p99: rd, note: fmt.Sprintf("(%.0f MB/s)", float64(size)/1e6/rd.Seconds())})
	return out, nil
}

// --- the fsync floor ---

type floor struct{ p50, p99, linkP50 time.Duration }

// fsyncFloor is what the disk under dir takes to make 4 KiB durable: a
// write and an fsync, and the same with the link and the directory fsync
// a blob/local Put adds.
func fsyncFloor(dir string) (floor, error) {
	d, err := os.MkdirTemp(dir, "fsync-")
	if err != nil {
		return floor{}, err
	}
	defer os.RemoveAll(d)
	buf := make([]byte, 4096)
	var plain, linked []time.Duration
	for i := range 200 {
		t0 := time.Now()
		p := filepath.Join(d, fmt.Sprintf("f%d", i))
		f, err := os.Create(p)
		if err != nil {
			return floor{}, err
		}
		if _, err := f.Write(buf); err != nil {
			return floor{}, err
		}
		if err := f.Sync(); err != nil {
			return floor{}, err
		}
		_ = f.Close()
		plain = append(plain, time.Since(t0))
		t1 := time.Now()
		f, err = os.Create(p + "t")
		if err != nil {
			return floor{}, err
		}
		_, _ = f.Write(buf)
		_ = f.Sync()
		_ = f.Close()
		if err := os.Link(p+"t", p+"l"); err != nil {
			return floor{}, err
		}
		df, err := os.Open(d)
		if err != nil {
			return floor{}, err
		}
		_ = df.Sync()
		_ = df.Close()
		linked = append(linked, time.Since(t1))
	}
	return floor{p50: percentile(plain, .5).Round(time.Microsecond), p99: percentile(plain, .99).Round(time.Microsecond),
		linkP50: percentile(linked, .5).Round(time.Microsecond)}, nil
}

func journalMode(s string) packstore.JournalMode {
	switch s {
	case "on":
		return packstore.JournalOn
	case "off":
		return packstore.JournalOff
	}
	return packstore.JournalDefault
}
