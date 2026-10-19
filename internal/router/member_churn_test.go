// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse-biscuit/biscuit-go/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/google/agentmesh/api"
	"github.com/google/agentmesh/internal/router"
	"github.com/google/agentmesh/internal/standalone"
)

// upstreamLeaksEnv runs the checks that fail on the libp2p releases go.mod
// pins today. They are skipped by default so CI stays green, and are meant
// to run unconditionally in the change that bumps both modules to releases
// carrying libp2p/go-libp2p-pubsub#735 and libp2p/go-libp2p#3559.
const upstreamLeaksEnv = "AGENTMESH_TEST_UPSTREAM_LEAKS"

const (
	// churnMembers is how many members join and leave: enough that a
	// per-member leak stands far above the noise, few enough to stay fast.
	churnMembers = 50
	// routerGoroutineTolerance absorbs the router's own background work
	// (DHT refresh, timers) between the baseline and the final count. A
	// per-member leak is churnMembers or more.
	routerGoroutineTolerance = 5
	// addrBookGCInterval is how often pstoremem garbage collects expired
	// addrs. It runs on a real-time ticker the mock clock does not drive, so
	// the peerstore check waits up to one interval of wall time.
	addrBookGCInterval = time.Minute
	// pollInterval is how often a wait re-checks its condition.
	pollInterval = 50 * time.Millisecond
)

// routerLabel marks every goroutine the router starts, and every goroutine
// those start, so the router's count can be told apart from the members'
// in one process.
var routerLabel = pprof.Labels("agentmesh_test", "router")

// TestRouterReleasesDepartedMembers is the regression test for item 1 of
// #277: router memory that stays allocated after members leave. Members
// join an in-process router, authenticate, gossip and leave; afterwards
// nothing the router holds per member may remain. The goroutine check
// passes today. The gossipsub and peerstore checks pin the two upstream
// leaks behind that item, and fail until go.mod picks up
// libp2p/go-libp2p-pubsub#735 and libp2p/go-libp2p#3559; set
// AGENTMESH_TEST_UPSTREAM_LEAKS=1 to run them.
func TestRouterReleasesDepartedMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clock := &skewedClock{}
	ps, err := pstoremem.NewPeerstore(pstoremem.WithClock(clock))
	if err != nil {
		t.Fatalf("pstoremem.NewPeerstore: %v", err)
	}
	gossipCtx := &childCountingContext{}
	router.SetStartHooks(t, ps, func(parent context.Context) context.Context {
		gossipCtx.Context = parent
		return gossipCtx
	})

	signer := writeSigningKey(t)
	srv, err := standalone.New(standalone.Options{
		BindAddress:  "127.0.0.1:0",
		DataDir:      t.TempDir(),
		ControlPlane: standalone.ControlPlaneTunables{SigningKeyPath: signer.path},
	})
	if err != nil {
		t.Fatalf("standalone.New: %v", err)
	}
	pprof.Do(ctx, routerLabel, func(ctx context.Context) {
		err = srv.Start(ctx)
	})
	if err != nil {
		t.Fatalf("standalone start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	routerAddr := routerAddrInfo(t, srv)

	baseGoroutines := settledRouterGoroutines(t, ctx)
	baseContexts := gossipCtx.liveChildren()
	t.Logf("baseline: %d router goroutines, %d gossipsub child contexts", sum(baseGoroutines), baseContexts)

	// Join: every member authenticates and gossip reaches it. The router
	// opens one outbound gossipsub stream per member, each on a context
	// derived from the one gossipsub was built with, and identify files
	// each member's addrs in the host's peerstore.
	start := time.Now()
	members := joinMembers(t, ctx, signer.key, routerAddr, churnMembers)
	waitFor(t, ctx, 10*time.Second, func() (bool, string) {
		got := gossipCtx.liveChildren() - baseContexts
		return got >= churnMembers, fmt.Sprintf("gossipsub child contexts above baseline = %d, want at least %d", got, churnMembers)
	})
	waitFor(t, ctx, 10*time.Second, func() (bool, string) {
		filed := ps.PeersWithAddrs()
		missing := 0
		for _, m := range members {
			if !slices.Contains(filed, m.ID()) {
				missing++
			}
		}
		return missing == 0, fmt.Sprintf("%d of %d members have no addrs in the router's peerstore", missing, len(members))
	})
	joined := time.Since(start)
	// The label filter sees the router's per-member work, so a leaked
	// goroutine per member would show in the check below.
	attached := sum(routerGoroutines(t))
	if want := sum(baseGoroutines) + churnMembers; attached < want {
		t.Fatalf("router goroutines with %d members attached = %d, want at least %d", churnMembers, attached, want)
	}
	t.Logf("%d members joined in %v; %d router goroutines while attached", churnMembers, joined.Round(time.Millisecond), attached)

	// Leave: each member closes its host, and with it every connection.
	departed := make([]peer.ID, 0, len(members))
	for _, m := range members {
		departed = append(departed, m.ID())
		m.leave()
	}

	t.Run("router goroutines return to baseline", func(t *testing.T) {
		var final map[string]int
		waitFor(t, ctx, 10*time.Second, func() (bool, string) {
			final = routerGoroutines(t)
			return sum(final) <= sum(baseGoroutines)+routerGoroutineTolerance,
				fmt.Sprintf("router goroutines = %d after %d members left, want at most %d (baseline %d + %d); grown stacks:\n%s",
					sum(final), churnMembers, sum(baseGoroutines)+routerGoroutineTolerance, sum(baseGoroutines), routerGoroutineTolerance, grown(baseGoroutines, final))
		})
	})

	t.Run("gossipsub releases per-peer contexts", func(t *testing.T) {
		skipUnlessUpstreamLeaks(t, "go-libp2p-pubsub v0.18.0", "libp2p/go-libp2p-pubsub#735", "the writer context of every departed peer stays registered with the PubSub context")
		waitFor(t, ctx, 10*time.Second, func() (bool, string) {
			got := gossipCtx.liveChildren()
			return got == baseContexts, fmt.Sprintf("gossipsub child contexts after %d members left = %d, want the baseline %d", churnMembers, got, baseContexts)
		})
	})

	t.Run("peerstore drops departed members after RecentlyConnectedAddrTTL", func(t *testing.T) {
		skipUnlessUpstreamLeaks(t, "go-libp2p v0.49.0", "libp2p/go-libp2p#3559", "addrs downgraded from ConnectedAddrTTL never enter the expiry heap, so the addr book keeps them past their TTL")
		// On a member's last disconnect identify moves its addrs from
		// ConnectedAddrTTL to RecentlyConnectedAddrTTL; past that TTL the
		// addr book's gc must drop them. The clock moves on every poll, so
		// an addr downgraded after an earlier move expires too, and the
		// poll outlasts the addr book's next gc.
		waitFor(t, ctx, addrBookGCInterval+15*time.Second, func() (bool, string) {
			clock.advance(peerstore.RecentlyConnectedAddrTTL + time.Second)
			held := 0
			for _, p := range ps.PeersWithAddrs() {
				if slices.Contains(departed, p) {
					held++
				}
			}
			return held == 0, fmt.Sprintf("%d of %d departed members still have addrs in the router's peerstore past RecentlyConnectedAddrTTL and the addr book's gc", held, len(departed))
		})
	})
}

// skipUnlessUpstreamLeaks skips a check that fails on the release go.mod
// pins until the upstream fix is released and picked up.
func skipUnlessUpstreamLeaks(t *testing.T, pinned, fix, leak string) {
	t.Helper()
	if os.Getenv(upstreamLeaksEnv) == "" {
		t.Skipf("fails on %s until %s is released and picked up: %s; set %s=1 to run", pinned, fix, leak, upstreamLeaksEnv)
	}
}

// signingKey is the control plane's Biscuit signing key, supplied as the
// operator's, so the test can mint member credentials without one
// enrollment round trip per member.
type signingKey struct {
	key  ed25519.PrivateKey
	path string
}

func writeSigningKey(t *testing.T) signingKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// The file must also carry the P-256 key the control plane signs its
	// JWTs with; the test has no use for it.
	es256, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var keys []byte
	for _, k := range []any{priv, es256} {
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})...)
	}
	path := filepath.Join(t.TempDir(), "signing-keys.pem")
	if err := os.WriteFile(path, keys, 0o600); err != nil {
		t.Fatal(err)
	}
	return signingKey{key: priv, path: path}
}

// routerAddrInfo is the embedded router's single-port WebSocket listener.
func routerAddrInfo(t *testing.T, srv *standalone.Server) peer.AddrInfo {
	t.Helper()
	id, err := peer.Decode(srv.PeerID())
	if err != nil {
		t.Fatalf("router peer ID %q: %v", srv.PeerID(), err)
	}
	ip, port, err := net.SplitHostPort(srv.Addr())
	if err != nil {
		t.Fatalf("router address %q: %v", srv.Addr(), err)
	}
	addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s/ws", ip, port))
	if err != nil {
		t.Fatal(err)
	}
	return peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{addr}}
}

// member is an in-process mesh member: a libp2p host that runs gossipsub
// and has authenticated to the router.
type member struct {
	host.Host
	cancel context.CancelFunc
}

func (m *member) leave() {
	m.cancel()
	_ = m.Close()
}

// joinMembers starts n members at once, each of which connects to the
// router and completes the mesh handshake with a credential signer issued.
func joinMembers(t *testing.T, ctx context.Context, signer ed25519.PrivateKey, routerAddr peer.AddrInfo, n int) []*member {
	t.Helper()
	members := make([]*member, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range members {
		wg.Go(func() { members[i], errs[i] = joinMember(ctx, signer, routerAddr) })
	}
	wg.Wait()
	for i, m := range members {
		if m != nil {
			t.Cleanup(m.leave)
		}
		if errs[i] != nil {
			t.Fatalf("member %d: %v", i, errs[i])
		}
	}
	return members
}

func joinMember(ctx context.Context, signer ed25519.PrivateKey, routerAddr peer.AddrInfo) (*member, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	h, err := libp2p.New(libp2p.Identity(priv), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		return nil, fmt.Errorf("libp2p.New: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	m := &member{Host: h, cancel: cancel}
	if _, err := pubsub.NewGossipSub(ctx, h); err != nil {
		m.leave()
		return nil, fmt.Errorf("gossipsub: %w", err)
	}
	cred, err := mintMemberBiscuit(signer, h.ID())
	if err != nil {
		m.leave()
		return nil, err
	}
	if err := h.Connect(ctx, routerAddr); err != nil {
		m.leave()
		return nil, fmt.Errorf("connect to router: %w", err)
	}
	if err := authenticate(ctx, h, routerAddr.ID, cred); err != nil {
		m.leave()
		return nil, err
	}
	return m, nil
}

// mintMemberBiscuit issues the credential the control plane would after a
// bootstrap enrollment: bound to the member's peer ID, with the node role.
func mintMemberBiscuit(signer ed25519.PrivateKey, id peer.ID) ([]byte, error) {
	builder := biscuit.NewBuilder(signer)
	for _, f := range []biscuit.Fact{
		{Predicate: biscuit.Predicate{Name: api.FactNode, IDs: []biscuit.Term{biscuit.String(id.String())}}},
		{Predicate: biscuit.Predicate{Name: api.FactRole, IDs: []biscuit.Term{biscuit.String(api.RoleNode)}}},
		{Predicate: biscuit.Predicate{Name: api.FactExpiration, IDs: []biscuit.Term{biscuit.Date(time.Now().Add(time.Hour))}}},
	} {
		if err := builder.AddAuthorityFact(f); err != nil {
			return nil, fmt.Errorf("biscuit fact: %w", err)
		}
	}
	tok, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("build biscuit: %w", err)
	}
	return tok.Serialize()
}

// authenticate runs the member side of the mesh handshake.
func authenticate(ctx context.Context, h host.Host, routerID peer.ID, cred []byte) error {
	s, err := h.NewStream(ctx, routerID, api.AuthProtocolID)
	if err != nil {
		return fmt.Errorf("open auth stream: %w", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	frame, err := proto.Marshal(&api.AuthFrame{Biscuit: cred})
	if err != nil {
		return err
	}
	if err := msgio.NewVarintWriter(s).WriteMsg(frame); err != nil {
		return fmt.Errorf("write auth frame: %w", err)
	}
	reader := msgio.NewVarintReaderSize(s, 64*1024)
	msg, err := reader.ReadMsg()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	defer reader.ReleaseMsg(msg)
	var resp api.AuthResponse
	if err := proto.Unmarshal(msg, &resp); err != nil {
		return fmt.Errorf("auth response: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("router refused the handshake: %s", resp.GetError())
	}
	return nil
}

// skewedClock is wall time moved forward by however much the test
// advanced it, the clock the router's peerstore expires addrs by.
type skewedClock struct{ offset atomic.Int64 }

func (c *skewedClock) Now() time.Time { return time.Now().Add(time.Duration(c.offset.Load())) }

func (c *skewedClock) advance(d time.Duration) { c.offset.Add(int64(d)) }

// childCountingContext counts the child contexts currently derived from it,
// the approach of the test in libp2p/go-libp2p-pubsub#735. It hides its
// parent's cancelCtx identity (Value returns nil), so Go registers each
// child through AfterFunc and calls the returned stop when the child is
// cancelled: live is the number of children not yet cancelled.
type childCountingContext struct {
	context.Context
	mu   sync.Mutex
	live int
}

func (c *childCountingContext) Value(any) any { return nil }

func (c *childCountingContext) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	c.live++
	c.mu.Unlock()
	stop := context.AfterFunc(c.Context, f)
	return func() bool {
		stopped := stop()
		if stopped {
			c.mu.Lock()
			c.live--
			c.mu.Unlock()
		}
		return stopped
	}
}

func (c *childCountingContext) liveChildren() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live
}

// routerGoroutines counts the goroutines that carry routerLabel, keyed by
// the first function on their stack outside the runtime, from the debug=1
// goroutine profile.
func routerGoroutines(t *testing.T) map[string]int {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		t.Fatalf("goroutine profile: %v", err)
	}
	counts := map[string]int{}
	for _, group := range strings.Split(buf.String(), "\n\n") {
		lines := strings.Split(strings.TrimSpace(group), "\n")
		if len(lines) < 3 || !strings.HasPrefix(lines[1], "# labels: ") || !strings.Contains(lines[1], `"agentmesh_test":"router"`) {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(lines[0], "%d @", &n); err != nil {
			continue
		}
		counts[topFunction(lines[2:])] += n
	}
	return counts
}

// topFunction names a stack by its first frame outside the runtime.
func topFunction(frames []string) string {
	for _, f := range frames {
		fields := strings.Fields(strings.TrimPrefix(f, "#"))
		if len(fields) < 2 {
			continue
		}
		fn, _, _ := strings.Cut(fields[1], "+0x")
		if !strings.HasPrefix(fn, "runtime.") && !strings.HasPrefix(fn, "internal/") && !strings.HasPrefix(fn, "sync.") {
			return fn
		}
	}
	return "unknown"
}

// settledRouterGoroutines waits until the router's goroutine count holds
// still for a second, its startup work done, and returns it.
func settledRouterGoroutines(t *testing.T, ctx context.Context) map[string]int {
	t.Helper()
	const steady = 20
	var last map[string]int
	same := 0
	waitFor(t, ctx, 15*time.Second, func() (bool, string) {
		now := routerGoroutines(t)
		if last != nil && sum(now) == sum(last) {
			same++
		} else {
			same = 0
		}
		last = now
		return same >= steady, fmt.Sprintf("router goroutines did not settle; last count %d", sum(now))
	})
	return last
}

func sum(counts map[string]int) int {
	total := 0
	for _, n := range counts {
		total += n
	}
	return total
}

// grown lists the stacks with more goroutines in now than in base.
func grown(base, now map[string]int) string {
	var lines []string
	for fn, n := range now {
		if n > base[fn] {
			lines = append(lines, fmt.Sprintf("  %+d %s", n-base[fn], fn))
		}
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// waitFor polls cond until it holds, and fails the test with cond's last
// message when timeout passes first.
func waitFor(t *testing.T, ctx context.Context, timeout time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		ok, msg := cond()
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", msg, ctx.Err())
		case <-deadline.C:
			t.Fatalf("after %v: %s", timeout, msg)
		case <-tick.C:
		}
	}
}
