package database

import "testing"

func TestParseClickHouse(t *testing.T) {
	opts, err := parseClickHouse("clickhouse://default:secret@ch.internal:9000/analytics")
	if err != nil {
		t.Fatal(err)
	}
	if opts.Auth.Username != "default" || opts.Auth.Password != "secret" {
		t.Fatalf("auth = %#v", opts.Auth)
	}
	if opts.Auth.Database != "analytics" {
		t.Fatalf("database = %s", opts.Auth.Database)
	}
	if len(opts.Addr) != 1 || opts.Addr[0] != "ch.internal:9000" {
		t.Fatalf("addr = %#v", opts.Addr)
	}
}
