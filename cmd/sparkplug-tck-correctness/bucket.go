package main

import (
	"fmt"
	"unicode/utf16"

	"github.com/cespare/xxhash/v2"
)

// HiveMQ runs every extension callback for a client (interceptors,
// authenticators) on one single-threaded task executor, picked by
// hashing the client ID into N buckets. N defaults to the CPU count.
//
// Several upstream TCK tests connect a helper client (a Host
// Application or Edge Node) synchronously from their constructor, and
// the constructor runs inside the NEW_TEST publish interceptor — on the
// collector's bucket. The helper's CONNECT has to be intercepted on the
// helper's own bucket, so when the two IDs share a bucket the
// interceptor waits on itself forever and NEW_TEST is never acked. With
// N=4 on CI runners that's a 1-in-4 chance per helper-creating test.
//
// The helper IDs are derived from the host/edge IDs we pass in, so we
// pick those IDs to keep every helper out of the collector's bucket.

// hivemqBucket mirrors HiveMQ CE's BucketUtils.getBucket:
//
//	Math.abs((int) (LongHashFunction.xx().hashChars(id) % bucketCount))
//
// hashChars hashes the string's UTF-16 code units as little-endian
// bytes with XXH64, seed 0.
func hivemqBucket(id string, bucketCount int) int {
	units := utf16.Encode([]rune(id))
	b := make([]byte, 0, 2*len(units))
	for _, u := range units {
		b = append(b, byte(u), byte(u>>8))
	}
	r := int32(int64(xxhash.Sum64(b)) % int64(bucketCount))
	if r < 0 {
		r = -r
	}
	return int(r)
}

// helperClientIDs are the client IDs the upstream TCK uses for the
// helper clients it may create for a test run with these IDs
// (utility/HostApplication.java, utility/EdgeNode.java).
func helperClientIDs(host, group, edge string) []string {
	return []string{
		"Sparkplug_TCK_" + host,
		"Sparkplug_TCK_" + host + "_WRONG", // edge/PrimaryHostTest
		"Sparkplug TCK " + group + " " + edge,
	}
}

// Executor counts to stay clear of: HiveMQ sizes the pool to the CPU
// count, so this covers any runner up to 64 cores.
const minBuckets, maxBuckets = 2, 64

func sharesBucket(a, b string) bool {
	for n := minBuckets; n <= maxBuckets; n++ {
		if hivemqBucket(a, n) == hivemqBucket(b, n) {
			return true
		}
	}
	return false
}

// pickRunIDs returns host and edge IDs for test i whose helper clients
// can't share an executor bucket with collectorID. The first candidate
// keeps the historical "<prefix><suffix>-<i>" form.
func pickRunIDs(collectorID, hostPrefix, group, edgePrefix, suffix string, i int) (host, edge string, err error) {
	for attempt := 0; attempt < 1_000_000; attempt++ {
		tag := fmt.Sprintf("%s-%d", suffix, i)
		if attempt > 0 {
			tag = fmt.Sprintf("%s-%d-%d", suffix, i, attempt)
		}
		host, edge = hostPrefix+tag, edgePrefix+tag
		clear := true
		for _, id := range helperClientIDs(host, group, edge) {
			if sharesBucket(id, collectorID) {
				clear = false
				break
			}
		}
		if clear {
			return host, edge, nil
		}
	}
	return "", "", fmt.Errorf("no host/edge IDs clear of collector %q's executor bucket", collectorID)
}
