package main

import "testing"

// Reference values from HiveMQ CE 2021.1's own BucketUtils.getBucket,
// run against the hivemq.jar the parity sweep boots.
func TestHivemqBucketMatchesHiveMQ(t *testing.T) {
	cases := []struct {
		id      string
		b4, b32 int
	}{
		{"Sparkplug_TCK_TCKHost1790317784060926974-0_WRONG", 0, 0},
		{"sparkplug-tck-correctness-1790317784060926974", 3, 3},
		{"a", 2, 10},
		{"", 3, 7},
		{"Sparkplug TCK TCKGroup TCKEdge1-0", 3, 31},
	}
	for _, c := range cases {
		if got := hivemqBucket(c.id, 4); got != c.b4 {
			t.Errorf("bucket(%q, 4) = %d, want %d", c.id, got, c.b4)
		}
		if got := hivemqBucket(c.id, 32); got != c.b32 {
			t.Errorf("bucket(%q, 32) = %d, want %d", c.id, got, c.b32)
		}
	}
}

func TestPickRunIDsAvoidsCollectorBucket(t *testing.T) {
	const collector = "sparkplug-tck-correctness-1790317784060926974"
	for i := 0; i < 11; i++ {
		host, edge, err := pickRunIDs(collector, "TCKHost", "TCKGroup", "TCKEdge", "1790317784060926974", i)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range helperClientIDs(host, "TCKGroup", edge) {
			for n := minBuckets; n <= maxBuckets; n++ {
				if hivemqBucket(id, n) == hivemqBucket(collector, n) {
					t.Fatalf("test %d: helper %q shares bucket %d/%d with collector", i, id, hivemqBucket(id, n), n)
				}
			}
		}
	}
}
