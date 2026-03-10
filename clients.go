package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

type client interface {
	do() (code int, usTaken uint64, err error)
}

func randomIPv4(r *rand.Rand) string {
	return fmt.Sprintf("%d.%d.%d.%d",
		r.Intn(256),
		r.Intn(256),
		r.Intn(256),
		r.Intn(256))
}

func ipStringFromUint32(ip uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d",
		byte(ip>>24),
		byte(ip>>16),
		byte(ip>>8),
		byte(ip),
	)
}

func randomIPv4Generator(seed *int64) func() string {
	seedValue := time.Now().UnixNano()
	if seed != nil {
		seedValue = *seed
	}
	r := rand.New(rand.NewSource(seedValue))
	var mx sync.Mutex
	return func() string {
		mx.Lock()
		ip := randomIPv4(r)
		mx.Unlock()
		return ip
	}
}

func randomIPv4UniqueSequenceGenerator(seed *int64) func() string {
	seedValue := time.Now().UnixNano()
	if seed != nil {
		seedValue = *seed
	}
	start := uint32(seedValue)
	step := uint32(uint64(seedValue>>32) | 1)
	counter := uint64(0)
	return func() string {
		n := atomic.AddUint64(&counter, 1) - 1
		return ipStringFromUint32(start + uint32(n)*step)
	}
}

func randomIPv4GeneratorWithCardinality(cardinality uint64, seed *int64) func() string {
	seq := randomIPv4UniqueSequenceGenerator(seed)
	ipPool := make([]string, int(cardinality))
	for i := range ipPool {
		ipPool[i] = seq()
	}
	counter := uint64(0)
	return func() string {
		n := atomic.AddUint64(&counter, 1) - 1
		return ipPool[n%uint64(len(ipPool))]
	}
}

type bodyStreamProducer func() (io.ReadCloser, error)

type clientOpts struct {
	HTTP2 bool

	maxConns          uint64
	timeout           time.Duration
	tlsConfig         *tls.Config
	disableKeepAlives bool

	requestURL *url.URL
	headers    *headersList
	method     string

	body                      *string
	bodProd                   bodyStreamProducer
	randomClientIP            bool
	randomClientIPCardinality *uint64
	randomClientIPSeed        *int64

	bytesRead, bytesWritten *int64
}

type fasthttpClient struct {
	client *fasthttp.Client

	headers *fasthttp.RequestHeader
	uri     *fasthttp.URI
	method  string

	body                    *string
	bodProd                 bodyStreamProducer
	randomClientIPGenerator func() string
}

func newFastHTTPClient(opts *clientOpts) client {
	c := new(fasthttpClient)
	uri := fasthttp.AcquireURI()
	if err := uri.Parse(
		[]byte(opts.requestURL.Host),
		[]byte(opts.requestURL.String()),
	); err != nil {
		// opts.requestURL must always be valid
		panic(err)
	}
	c.uri = uri
	c.client = &fasthttp.Client{
		MaxConnsPerHost:               int(opts.maxConns),
		ReadTimeout:                   opts.timeout,
		WriteTimeout:                  opts.timeout,
		DisableHeaderNamesNormalizing: true,
		TLSConfig:                     opts.tlsConfig,
		Dial: fasthttpDialFunc(
			opts.bytesRead, opts.bytesWritten,
			opts.timeout,
		),
	}
	c.headers = headersToFastHTTPHeaders(opts.headers)
	c.method, c.body = opts.method, opts.body
	c.bodProd = opts.bodProd
	if opts.randomClientIP {
		if opts.randomClientIPCardinality != nil {
			c.randomClientIPGenerator = randomIPv4GeneratorWithCardinality(
				*opts.randomClientIPCardinality,
				opts.randomClientIPSeed,
			)
		} else {
			c.randomClientIPGenerator = randomIPv4Generator(opts.randomClientIPSeed)
		}
	}
	return client(c)
}

func (c *fasthttpClient) do() (
	code int, usTaken uint64, err error,
) {
	// prepare the request
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	if c.headers != nil {
		c.headers.CopyTo(&req.Header)
	}
	if c.randomClientIPGenerator != nil {
		req.Header.Set("X-Client-IP", c.randomClientIPGenerator())
	}
	req.Header.SetMethod(c.method)
	req.SetURI(c.uri)
	req.UseHostHeader = true
	if c.body != nil {
		req.SetBodyString(*c.body)
	} else {
		bs, bserr := c.bodProd()
		if bserr != nil {
			return 0, 0, bserr
		}
		req.SetBodyStream(bs, -1)
	}

	// fire the request
	start := time.Now()
	err = c.client.Do(req, resp)
	if err != nil {
		code = -1
	} else {
		code = resp.StatusCode()
	}
	usTaken = uint64(time.Since(start).Nanoseconds() / 1000)

	// release resources
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)

	return
}

type httpClient struct {
	client *http.Client

	headers http.Header
	url     *url.URL
	method  string

	body                    *string
	bodProd                 bodyStreamProducer
	randomClientIPGenerator func() string
}

func newHTTPClient(opts *clientOpts) client {
	c := new(httpClient)
	tr := &http.Transport{
		TLSClientConfig:     opts.tlsConfig,
		MaxIdleConnsPerHost: int(opts.maxConns),
		DisableKeepAlives:   opts.disableKeepAlives,
		ForceAttemptHTTP2:   opts.HTTP2,
		DialContext:         httpDialContextFunc(opts.bytesRead, opts.bytesWritten, opts.timeout),
	}

	cl := &http.Client{
		Transport: tr,
		Timeout:   opts.timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	c.client = cl

	c.headers = headersToHTTPHeaders(opts.headers)
	c.method, c.body, c.bodProd = opts.method, opts.body, opts.bodProd
	c.url = opts.requestURL
	if opts.randomClientIP {
		if opts.randomClientIPCardinality != nil {
			c.randomClientIPGenerator = randomIPv4GeneratorWithCardinality(
				*opts.randomClientIPCardinality,
				opts.randomClientIPSeed,
			)
		} else {
			c.randomClientIPGenerator = randomIPv4Generator(opts.randomClientIPSeed)
		}
	}

	return client(c)
}

func (c *httpClient) do() (
	code int, usTaken uint64, err error,
) {
	req := &http.Request{}

	req.Header = c.headers
	req.Method = c.method
	req.URL = c.url
	if c.randomClientIPGenerator != nil {
		req.Header.Set("X-Client-IP", c.randomClientIPGenerator())
	}

	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	if c.body != nil {
		br := strings.NewReader(*c.body)
		req.ContentLength = int64(len(*c.body))
		req.Body = ioutil.NopCloser(br)
	} else {
		bs, bserr := c.bodProd()
		if bserr != nil {
			return 0, 0, bserr
		}
		req.Body = bs
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		code = -1
	} else {
		code = resp.StatusCode

		_, berr := io.Copy(ioutil.Discard, resp.Body)
		if berr != nil {
			err = berr
		}

		if cerr := resp.Body.Close(); cerr != nil {
			err = cerr
		}
	}
	usTaken = uint64(time.Since(start).Nanoseconds() / 1000)

	return
}

func headersToFastHTTPHeaders(h *headersList) *fasthttp.RequestHeader {
	if len(*h) == 0 {
		return nil
	}
	res := new(fasthttp.RequestHeader)
	for _, header := range *h {
		res.Set(header.key, header.value)
	}
	return res
}

func headersToHTTPHeaders(h *headersList) http.Header {
	if len(*h) == 0 {
		return http.Header{}
	}
	headers := http.Header{}

	for _, header := range *h {
		headers[header.key] = []string{header.value}
	}
	return headers
}
