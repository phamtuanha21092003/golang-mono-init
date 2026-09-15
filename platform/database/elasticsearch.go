package database

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/pkg/errors"
)

// ErrDocumentNotFound is returned when Elasticsearch answers 404 for a document.
var ErrDocumentNotFound = errors.New("elasticsearch document not found")

// Elasticsearch is the official client. Nethub used olivere/elastic, which is unmaintained.
// Callers send query DSL as JSON instead of the old typed query builder.
type Elasticsearch struct {
	Client *elasticsearch.Client
}

// OpenElasticsearch accepts one URL or a comma-separated list.
// A URL may include basic-auth user info: http://user:pass@host:9200.
func OpenElasticsearch(raw string) (*Elasticsearch, error) {
	addresses, username, password, err := parseElasticURL(raw)
	if err != nil {
		return nil, err
	}

	opts := []elasticsearch.Option{elasticsearch.WithAddresses(addresses...)}
	if username != "" {
		opts = append(opts, elasticsearch.WithBasicAuth(username, password))
	}

	client, err := elasticsearch.New(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "open elasticsearch")
	}
	es := &Elasticsearch{Client: client}
	if err := es.Ping(context.Background()); err != nil {
		return nil, err
	}
	return es, nil
}

func (e *Elasticsearch) Ping(ctx context.Context) error {
	if e == nil || e.Client == nil {
		return errors.New("elasticsearch is not connected")
	}
	res, err := e.Client.Ping(e.Client.Ping.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "elasticsearch ping")
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch ping: %s", res.Status())
	}
	return nil
}

func (e *Elasticsearch) Index(ctx context.Context, index, id string, document any) error {
	body, err := json.Marshal(document)
	if err != nil {
		return errors.Wrap(err, "marshal elasticsearch document")
	}
	opts := []func(*esapi.IndexRequest){e.Client.Index.WithContext(ctx)}
	if id != "" {
		opts = append(opts, e.Client.Index.WithDocumentID(id))
	}
	res, err := e.Client.Index(index, bytes.NewReader(body), opts...)
	if err != nil {
		return errors.Wrap(err, "elasticsearch index")
	}
	defer res.Body.Close()
	return elasticStatus(res)
}

func (e *Elasticsearch) Get(ctx context.Context, index, id string) ([]byte, error) {
	res, err := e.Client.Get(index, id, e.Client.Get.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "elasticsearch get")
	}
	defer res.Body.Close()
	if res.StatusCode == 404 {
		_, _ = io.Copy(io.Discard, res.Body)
		return nil, ErrDocumentNotFound
	}
	payload, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch get: %s", res.Status())
	}
	return payload, nil
}

func (e *Elasticsearch) Search(ctx context.Context, index string, query any) ([]byte, error) {
	body, err := json.Marshal(query)
	if err != nil {
		return nil, errors.Wrap(err, "marshal elasticsearch query")
	}
	res, err := e.Client.Search(
		e.Client.Search.WithContext(ctx),
		e.Client.Search.WithIndex(index),
		e.Client.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, errors.Wrap(err, "elasticsearch search")
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch search: %s", res.Status())
	}
	return payload, nil
}

func (e *Elasticsearch) Delete(ctx context.Context, index, id string) error {
	res, err := e.Client.Delete(index, id, e.Client.Delete.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "elasticsearch delete")
	}
	defer res.Body.Close()
	if res.StatusCode == 404 {
		return ErrDocumentNotFound
	}
	return elasticStatus(res)
}

func elasticStatus(res *esapi.Response) error {
	if res.IsError() {
		payload, _ := io.ReadAll(res.Body)
		if len(payload) == 0 {
			return fmt.Errorf("elasticsearch: %s", res.Status())
		}
		return fmt.Errorf("elasticsearch: %s: %s", res.Status(), payload)
	}
	return nil
}

func parseElasticURL(raw string) (addresses []string, username, password string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, "", "", errors.New("elasticsearch url is required")
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "://") {
			part = "http://" + part
		}
		parsed, parseErr := url.Parse(part)
		if parseErr != nil {
			return nil, "", "", errors.Wrap(parseErr, "parse elasticsearch url")
		}
		if parsed.Host == "" {
			return nil, "", "", errors.New("elasticsearch url is missing a host")
		}
		if parsed.User != nil && username == "" {
			username = parsed.User.Username()
			password, _ = parsed.User.Password()
			parsed.User = nil
		}
		addresses = append(addresses, parsed.String())
	}
	if len(addresses) == 0 {
		return nil, "", "", errors.New("elasticsearch url is required")
	}
	return addresses, username, password, nil
}
