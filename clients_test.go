package main

import (
	"bytes"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/goware/urlx"
)

func TestShouldReturnNilIfNoHeadersWhereSet(t *testing.T) {
	h := new(headersList)
	if headersToFastHTTPHeaders(h) != nil {
		t.Fail()
	}
}

func TestShouldReturnEmptyHeadersIfNoHeaadersWhereSet(t *testing.T) {
	h := new(headersList)
	if len(headersToHTTPHeaders(h)) != 0 {
		t.Fail()
	}
}

func TestShouldProperlyConvertToHttpHeaders(t *testing.T) {
	h := new(headersList)
	for _, hs := range []string{
		"Content-Type: application/json", "Custom-Header: xxx42xxx",
	} {
		if err := h.Set(hs); err != nil {
			t.Error(err)
		}
	}
	fh := headersToFastHTTPHeaders(h)
	{
		e, a := []byte("application/json"), fh.Peek("Content-Type")
		if !bytes.Equal(e, a) {
			t.Errorf("Expected %v, but got %v", e, a)
		}
	}
	if e, a := []byte("xxx42xxx"), fh.Peek("Custom-Header"); !bytes.Equal(e, a) {
		t.Errorf("Expected %v, but got %v", e, a)
	}

	nh := headersToHTTPHeaders(h)
	{
		e, a := "application/json", nh.Get("Content-Type")
		if e != a {
			t.Errorf("Expected %v, but got %v", e, a)
		}
	}
	if e, a := "xxx42xxx", nh.Get("Custom-Header"); e != a {
		t.Errorf("Expected %v, but got %v", e, a)
	}
}

func TestHTTP2Client(t *testing.T) {
	responseSize := 1024
	response := bytes.Repeat([]byte{'a'}, responseSize)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.ProtoAtLeast(2, 0) {
			t.Errorf("invalid HTTP proto version: %v", r.Proto)
		}

		w.WriteHeader(http.StatusOK)
		_, err := w.Write(response)
		if err != nil {
			t.Error(err)
		}
	}))
	s.EnableHTTP2 = true
	s.TLS = &tls.Config{
		InsecureSkipVerify: true,
	}
	s.StartTLS()
	defer s.Close()

	bytesRead, bytesWritten := int64(0), int64(0)
	requestURL, err := urlx.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := newHTTPClient(&clientOpts{
		HTTP2: true,

		headers:    new(headersList),
		requestURL: requestURL,
		method:     "GET",
		tlsConfig: &tls.Config{
			InsecureSkipVerify: true,
		},

		body: new(string),

		bytesRead:    &bytesRead,
		bytesWritten: &bytesWritten,
	})
	code, _, err := c.do()
	if err != nil {
		t.Error(err)
		return
	}
	if code != http.StatusOK {
		t.Errorf("invalid response code: %v", code)
	}
	if atomic.LoadInt64(&bytesRead) == 0 {
		t.Errorf("invalid response size: %v", bytesRead)
	}
	if atomic.LoadInt64(&bytesWritten) == 0 {
		t.Errorf("empty request of size: %v", bytesWritten)
	}
}

func TestHTTP1Clients(t *testing.T) {
	responseSize := 1024
	response := bytes.Repeat([]byte{'a'}, responseSize)
	s := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 1 {
				t.Errorf("invalid HTTP proto version: %v", r.Proto)
			}

			w.WriteHeader(http.StatusOK)
			_, err := w.Write(response)
			if err != nil {
				t.Error(err)
			}
		},
	))
	defer s.Close()

	bytesRead, bytesWritten := int64(0), int64(0)
	requestURL, err := urlx.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	cc := &clientOpts{
		HTTP2: false,

		headers:    new(headersList),
		requestURL: requestURL,
		method:     "GET",

		body: new(string),

		bytesRead:    &bytesRead,
		bytesWritten: &bytesWritten,
	}
	clients := []client{
		newHTTPClient(cc),
		newFastHTTPClient(cc),
	}
	for _, c := range clients {
		bytesRead, bytesWritten = 0, 0
		code, _, err := c.do()
		if err != nil {
			t.Error(err)
			return
		}
		if code != http.StatusOK {
			t.Errorf("invalid response code: %v", code)
		}
		if bytesRead == 0 {
			t.Errorf("invalid response size: %v", bytesRead)
		}
		if bytesWritten == 0 {
			t.Errorf("empty request of size: %v", bytesWritten)
		}
	}
}

func TestRandomIPv4GeneratorWithCardinality(t *testing.T) {
	seed := int64(42)
	g := randomIPv4GeneratorWithCardinality(3, &seed)
	seq := []string{g(), g(), g(), g(), g(), g()}

	expected := []string{seq[0], seq[1], seq[2], seq[0], seq[1], seq[2]}
	if !reflect.DeepEqual(seq, expected) {
		t.Fatalf("expected repeating sequence %v, got %v", expected, seq)
	}

	distinct := map[string]struct{}{}
	for _, ip := range seq[:3] {
		distinct[ip] = struct{}{}
	}
	if len(distinct) != 3 {
		t.Fatalf("expected 3 distinct IPs in cycle, got %d", len(distinct))
	}
}

func TestRandomIPv4GeneratorWithSeed(t *testing.T) {
	seed := int64(7)
	g1 := randomIPv4Generator(&seed)
	g2 := randomIPv4Generator(&seed)

	seq1 := []string{g1(), g1(), g1()}
	seq2 := []string{g2(), g2(), g2()}
	if !reflect.DeepEqual(seq1, seq2) {
		t.Fatalf("expected deterministic sequences to match, got %v and %v", seq1, seq2)
	}
}
