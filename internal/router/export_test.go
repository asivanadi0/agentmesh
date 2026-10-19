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

package router

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peerstore"
)

// SetStartHooks makes every router started until t ends build its host on
// ps and its gossipsub on the context pubsubContext returns, for a test in
// package router_test that starts the router through standalone. Either may
// be nil to keep the default. The tests that call it must not run in
// parallel.
func SetStartHooks(t testing.TB, ps peerstore.Peerstore, pubsubContext func(context.Context) context.Context) {
	t.Helper()
	prev := startHooks
	startHooks.peerstore = ps
	startHooks.pubsubContext = pubsubContext
	t.Cleanup(func() { startHooks = prev })
}
