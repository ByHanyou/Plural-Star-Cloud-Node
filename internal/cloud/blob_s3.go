// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

const (
	s3UnsignedPayload = "UNSIGNED-PAYLOAD"
	s3EmptyPayload    = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type s3Backend struct {
	endpoint  *url.URL
	region    string
	bucket    string
	prefix    string
	accessKey string
	secretKey string
	client    *http.Client
}

func newS3Backend(cfg config.CloudConfig) *s3Backend {
	u, err := url.Parse(cfg.S3Endpoint)
	if err != nil || u.Host == "" {
		u = &url.URL{Scheme: "https", Host: cfg.S3Endpoint}
	}
	region := cfg.S3Region
	if region == "" {
		region = "us-east-1"
	}
	return &s3Backend{
		endpoint:  u,
		region:    region,
		bucket:    cfg.S3Bucket,
		prefix:    strings.Trim(cfg.S3Prefix, "/"),
		accessKey: cfg.S3AccessKey,
		secretKey: cfg.S3SecretKey,
		client:    &http.Client{Timeout: 10 * time.Minute},
	}
}

func (b *s3Backend) Name() string { return config.MediaBackendS3 }

func (b *s3Backend) key(id string) string {
	if b.prefix == "" {
		return id
	}
	return b.prefix + "/" + id
}

func s3Encode(s string, keepSlash bool) string {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.', c == '_', c == '~':
			out.WriteByte(c)
		case c == '/' && keepSlash:
			out.WriteByte(c)
		default:
			fmt.Fprintf(&out, "%%%02X", c)
		}
	}
	return out.String()
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (b *s3Backend) sign(req *http.Request, payloadHash string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := now.UTC().Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	host := req.URL.Host
	req.Host = host

	canonicalURI := s3Encode(req.URL.Path, true)
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	query := req.URL.Query()
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var qs []string
	for _, k := range keys {
		vals := query[k]
		sort.Strings(vals)
		for _, v := range vals {
			qs = append(qs, s3Encode(k, false)+"="+s3Encode(v, false))
		}
	}
	canonicalQuery := strings.Join(qs, "&")

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonicalRequest := strings.Join([]string{
		req.Method, canonicalURI, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")

	scope := dateStamp + "/" + b.region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex(canonicalRequest)
	kDate := hmacSHA256([]byte("AWS4"+b.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, b.region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+b.accessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func (b *s3Backend) objectURL(id string) *url.URL {
	u := *b.endpoint
	u.Path = "/" + b.bucket + "/" + b.key(id)
	u.RawQuery = ""
	return &u
}

func (b *s3Backend) do(method string, u *url.URL, body io.Reader, size int64, payloadHash string) (*http.Response, error) {
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if body != nil && size >= 0 {
		req.ContentLength = size
	}
	b.sign(req, payloadHash, time.Now())
	return b.client.Do(req)
}

func (b *s3Backend) Put(id string, r io.Reader, size int64) error {
	resp, err := b.do(http.MethodPut, b.objectURL(id), r, size, s3UnsignedPayload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("s3 put %s: status %d: %s", id, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func (b *s3Backend) PutFile(id string, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	err = b.Put(id, f, st.Size())
	_ = f.Close()
	if err != nil {
		return err
	}
	return os.Remove(path)
}

func (b *s3Backend) Get(id string) (io.ReadCloser, int64, error) {
	resp, err := b.do(http.MethodGet, b.objectURL(id), nil, -1, s3EmptyPayload)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("s3 get %s: status %d", id, resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, nil
}

func (b *s3Backend) Head(id string) (int64, error) {
	resp, err := b.do(http.MethodHead, b.objectURL(id), nil, -1, s3EmptyPayload)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("s3 head %s: status %d", id, resp.StatusCode)
	}
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return size, nil
}

func (b *s3Backend) Delete(id string) error {
	resp, err := b.do(http.MethodDelete, b.objectURL(id), nil, -1, s3EmptyPayload)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("s3 delete %s: status %d", id, resp.StatusCode)
	}
	return nil
}

type s3ListResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	} `xml:"Contents"`
}

func (b *s3Backend) List(fn func(id string, size int64, modified time.Time) error) error {
	token := ""
	for {
		u := *b.endpoint
		u.Path = "/" + b.bucket
		q := url.Values{}
		q.Set("list-type", "2")
		q.Set("max-keys", "1000")
		if b.prefix != "" {
			q.Set("prefix", b.prefix+"/")
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		u.RawQuery = q.Encode()
		resp, err := b.do(http.MethodGet, &u, nil, -1, s3EmptyPayload)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return fmt.Errorf("s3 list: status %d", resp.StatusCode)
		}
		var result s3ListResult
		decodeErr := xml.NewDecoder(resp.Body).Decode(&result)
		_ = resp.Body.Close()
		if decodeErr != nil {
			return decodeErr
		}
		for _, c := range result.Contents {
			id := c.Key
			if b.prefix != "" {
				id = strings.TrimPrefix(id, b.prefix+"/")
			}
			if !validHex32(id) {
				continue
			}
			modified, _ := time.Parse(time.RFC3339, c.LastModified)
			if err := fn(id, c.Size, modified); err != nil {
				return err
			}
		}
		if !result.IsTruncated || result.NextContinuationToken == "" {
			return nil
		}
		token = result.NextContinuationToken
	}
}
