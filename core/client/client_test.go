package client

import (
	"errors"
	"net"
	"net/url"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

// 1. net.SplitHostPort
// 2. if error, "too many colons in address" => url.Parse => u.Host, ignore path and scheme
// 3. if error "missing port in address", net.AddrError.Addr and += ":8484"

func TestURLs(t *testing.T) {
	// host, port, err := net.SplitHostPort("127.0.0.1:5432")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log(host)
	// t.Log(port)
	host, port, err := net.SplitHostPort("/var/run/postgres")
	if err != nil {

		var addrErr *net.AddrError
		if errors.As(err, &addrErr) {
			t.Log(addrErr.Addr)
			t.Log(addrErr.Err)
		}
	}
	t.Log(host)
	t.Log(port)
	u, err := url.Parse("http://127.0.0.1:5432")
	if err != nil {
		t.Log(err)
	}
	spew.Config.DisableMethods = true
	spew.Dump(u)
	t.Log(u)
}

func Test_convertTuple(t *testing.T) {
	tests := []struct {
		name      string
		tuple     []any
		want      []string
		wantIsNil []bool
		wantErr   bool
	}{
		{
			"string",
			[]any{"woot"},
			[]string{"woot"},
			[]bool{false},
			false,
		},
		{
			"int",
			[]any{1},
			[]string{"1"},
			[]bool{false},
			false,
		},
		{
			"empty",
			nil,
			[]string{}, // not nil, presently
			[]bool{},
			false,
		},
		{
			"nil",
			[]any{nil},
			[]string{""},
			[]bool{true},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIsNil, err := convertTuple(tt.tuple)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertTuple() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertTuple() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(gotIsNil, tt.wantIsNil) {
				t.Errorf("convertTuple() gotIsNil = %v, want %v", gotIsNil, tt.wantIsNil)
			}
		})
	}
}
