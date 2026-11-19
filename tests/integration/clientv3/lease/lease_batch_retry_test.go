// Copyright 2026 The etcd Authors
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

package lease_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

// batchLeaseServer is an in-process LeaseServer whose LeaseKeepAlive
// streams fail a configurable number of times before they succeed. It
// records how many keep alive requests each stream receives so tests can
// assert that retries only resend the leases missing a response.
type batchLeaseServer struct {
	pb.UnimplementedLeaseServer

	mu        sync.Mutex
	failFirst int64
	failCode  codes.Code
	// succeedBeforeFail, when positive, makes a failing stream answer the
	// first succeedBeforeFail requests, then return the injected error upon
	// receiving the next request, simulating a stream that breaks halfway
	// through a batch.
	succeedBeforeFail int
	reqPerCall        [][]int64

	streamCalls atomic.Int64
}

func (s *batchLeaseServer) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	callIdx := s.streamCalls.Add(1) - 1

	var reqIDs []int64
	shouldFail := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return callIdx < s.failFirst
	}()
	srvErr := status.Error(s.failCode, "injected batch keepalive failure")
	finish := func(err error) error {
		s.storeRequests(callIdx, reqIDs)
		return err
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return finish(nil)
		}
		if err != nil {
			return finish(err)
		}
		reqIDs = append(reqIDs, req.ID)

		if shouldFail {
			if s.succeedBeforeFail > 0 && len(reqIDs) > s.succeedBeforeFail {
				return finish(srvErr)
			}
			if s.succeedBeforeFail == 0 {
				return finish(srvErr)
			}
		}

		if err = stream.Send(&pb.LeaseKeepAliveResponse{
			Header: &pb.ResponseHeader{},
			ID:     req.ID,
			TTL:    10,
		}); err != nil {
			return finish(err)
		}
	}
}

// storeRequests records a snapshot of the requests received by one stream.
func (s *batchLeaseServer) storeRequests(callIdx int64, reqIDs []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for int64(len(s.reqPerCall)) <= callIdx {
		s.reqPerCall = append(s.reqPerCall, nil)
	}
	recorded := make([]int64, len(reqIDs))
	copy(recorded, reqIDs)
	s.reqPerCall[callIdx] = recorded
}

func (s *batchLeaseServer) snapshotRequestCounts() [][]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]int64(nil), s.reqPerCall...)
}

func newBatchLeaseClient(t *testing.T, srv *batchLeaseServer) *clientv3.Client {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	pb.RegisterLeaseServer(grpcSrv, srv)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		grpcSrv.Serve(ln)
	}()

	t.Cleanup(func() {
		grpcSrv.Stop()
		<-serveDone
		ln.Close()
	})

	cli, err := integration.NewClient(t, clientv3.Config{
		Endpoints:   []string{ln.Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	return cli
}

// TestLeaseKeepAliveOnceBatchRetry verifies a batch renewal transparently
// retries on a recoverable stream error, resending only the leases that did
// not get a response, and still returns every lease grouped by ID.
func TestLeaseKeepAliveOnceBatchRetry(t *testing.T) {
	srv := &batchLeaseServer{failCode: codes.Unavailable, failFirst: 1}
	cli := newBatchLeaseClient(t, srv)

	ids := []clientv3.LeaseID{101, 202, 303}

	resps, err := cli.KeepAliveOnceBatch(t.Context(), ids)
	require.NoErrorf(t, err, "expected retry to succeed, got %v", err)
	require.Len(t, resps, len(ids))

	for _, id := range ids {
		resp, ok := resps[id]
		require.Truef(t, ok, "missing response for lease %d", id)
		require.Equalf(t, id, resp.ID, "response grouped under wrong lease ID")
		require.Positivef(t, resp.TTL, "lease %d got non-positive TTL", id)
	}

	require.Equalf(t, int64(2), srv.streamCalls.Load(),
		"expected one failed stream plus one retry stream, got %d", srv.streamCalls.Load())

	counts := srv.snapshotRequestCounts()
	require.Lenf(t, counts, 2, "expected requests recorded for two streams")
	require.NotEmptyf(t, counts[0], "first stream must have sent at least one keepalive request")
	require.Subsetf(t, ids, leaseIDSlice(counts[0]), "first stream must only send requested leases")
	require.Lenf(t, counts[1], len(ids), "retry must resend all leases since no response arrived")
}

// TestLeaseKeepAliveOnceBatchPartialRetry verifies that responses already
// received before a stream breaks are kept and only the missing leases are
// resent on the retry stream.
func TestLeaseKeepAliveOnceBatchPartialRetry(t *testing.T) {
	srv := &batchLeaseServer{
		failCode:          codes.Internal,
		failFirst:         1,
		succeedBeforeFail: 2,
	}
	cli := newBatchLeaseClient(t, srv)

	ids := []clientv3.LeaseID{101, 202, 303, 404}

	resps, err := cli.KeepAliveOnceBatch(t.Context(), ids)
	require.NoErrorf(t, err, "expected partial retry to succeed, got %v", err)
	require.Len(t, resps, len(ids))
	require.Equalf(t, int64(2), srv.streamCalls.Load(),
		"expected one failed stream plus one retry stream, got %d", srv.streamCalls.Load())

	counts := srv.snapshotRequestCounts()
	require.Lenf(t, counts, 2, "expected requests recorded for two streams")
	require.Subsetf(t, ids, leaseIDSlice(counts[0]), "first stream must only send requested leases")
	require.Lenf(t, counts[0], 3, "first stream answers two leases then breaks on the third")
	require.Lenf(t, counts[1], 2, "retry must resend only the two leases without a response")

	gotOnRetry := map[clientv3.LeaseID]struct{}{}
	for _, id := range counts[1] {
		gotOnRetry[clientv3.LeaseID(id)] = struct{}{}
	}
	require.Equal(t, map[clientv3.LeaseID]struct{}{303: {}, 404: {}}, gotOnRetry,
		"retry stream must only contain leases missing a response")

	// the two leases answered before the stream broke must not be resent
	for _, id := range counts[1] {
		require.NotContainsf(t, []clientv3.LeaseID{101, 202}, clientv3.LeaseID(id),
			"lease %d already answered must not be resent", id)
	}
}

func leaseIDSlice(ids []int64) []clientv3.LeaseID {
	out := make([]clientv3.LeaseID, len(ids))
	for i, id := range ids {
		out[i] = clientv3.LeaseID(id)
	}
	return out
}

// TestLeaseKeepAliveOnceBatchNonRetryableError verifies a non-recoverable
// error aborts the batch call instead of looping forever.
func TestLeaseKeepAliveOnceBatchNonRetryableError(t *testing.T) {
	srv := &batchLeaseServer{failCode: codes.InvalidArgument, failFirst: 100}
	cli := newBatchLeaseClient(t, srv)

	_, err := cli.KeepAliveOnceBatch(t.Context(), []clientv3.LeaseID{101, 202})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.Truef(t, ok, "expected a gRPC status error, got %T", err)
	require.Equalf(t, codes.InvalidArgument, st.Code(), "expected InvalidArgument, got %v", st.Code())
	require.Equalf(t, int64(1), srv.streamCalls.Load(),
		"non-recoverable error must not be retried, got %d streams", srv.streamCalls.Load())
}

// TestLeaseKeepAliveOnceBatchContextCanceled verifies cancellation while
// waiting for a retry returns promptly with the context error and stops
// opening further streams.
func TestLeaseKeepAliveOnceBatchContextCanceled(t *testing.T) {
	srv := &batchLeaseServer{failCode: codes.Unavailable, failFirst: 100}
	cli := newBatchLeaseClient(t, srv)

	ctx, cancel := context.WithCancel(t.Context())
	donec := make(chan error, 1)
	go func() {
		_, err := cli.KeepAliveOnceBatch(ctx, []clientv3.LeaseID{101})
		donec <- err
	}()

	require.Eventually(t, func() bool {
		return srv.streamCalls.Load() >= 1
	}, 5*time.Second, 50*time.Millisecond, "first keepalive stream was never opened")

	cancel()

	select {
	case err := <-donec:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("KeepAliveOnceBatch did not return after context cancellation")
	}

	require.Equalf(t, int64(1), srv.streamCalls.Load(),
		"canceled batch must not open a retry stream, got %d", srv.streamCalls.Load())
}
