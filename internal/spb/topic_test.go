package spb

import "testing"

func TestParseTopic(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    Topic
		wantErr bool
	}{
		{
			name: "node NBIRTH",
			in:   "spBv1.0/G1/NBIRTH/N1",
			want: Topic{Namespace: Namespace, EdgeNodeID: EdgeNodeID{Group: "G1", Node: "N1"}, Type: NBIRTH},
		},
		{
			name: "device DBIRTH",
			in:   "spBv1.0/G1/DBIRTH/N1/D1",
			want: Topic{Namespace: Namespace, EdgeNodeID: EdgeNodeID{Group: "G1", Node: "N1"}, Type: DBIRTH, Device: "D1"},
		},
		{
			name: "host STATE",
			in:   "spBv1.0/STATE/host1",
			want: Topic{Namespace: Namespace, Type: STATE, Host: "host1"},
		},
		{name: "wrong namespace", in: "spBv2.0/G/NBIRTH/N", wantErr: true},
		{name: "node msg with device segment", in: "spBv1.0/G/NDATA/N/D", wantErr: true},
		{name: "device msg without device segment", in: "spBv1.0/G/DDATA/N", wantErr: true},
		{name: "unknown message type", in: "spBv1.0/G/HELLO/N", wantErr: true},
		{name: "empty group", in: "spBv1.0//NBIRTH/N", wantErr: true},
		{name: "empty node", in: "spBv1.0/G/NBIRTH/", wantErr: true},
		{name: "reserved char in group", in: "spBv1.0/G+1/NBIRTH/N", wantErr: true},
		{name: "STATE missing host", in: "spBv1.0/STATE/", wantErr: true},
		{name: "STATE with extra segment", in: "spBv1.0/STATE/host1/extra", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTopic(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if rt := got.String(); rt != tc.in {
				t.Errorf("round-trip: %q != %q", rt, tc.in)
			}
		})
	}
}

func TestMessageTypeKinds(t *testing.T) {
	for _, mt := range []MessageType{NBIRTH, NDEATH, NDATA, NCMD, STATE} {
		if !mt.IsNode() {
			t.Errorf("%s should be node-level", mt)
		}
		if mt.IsDevice() {
			t.Errorf("%s must not be device-level", mt)
		}
	}
	for _, mt := range []MessageType{DBIRTH, DDEATH, DDATA, DCMD} {
		if !mt.IsDevice() {
			t.Errorf("%s should be device-level", mt)
		}
		if mt.IsNode() {
			t.Errorf("%s must not be node-level", mt)
		}
	}
}
