package database

import "testing"

func TestParseElasticURL(t *testing.T) {
	addresses, user, pass, err := parseElasticURL("http://elastic:secret@search.internal:9200")
	if err != nil {
		t.Fatal(err)
	}
	if user != "elastic" || pass != "secret" {
		t.Fatalf("auth = %s %s", user, pass)
	}
	if len(addresses) != 1 || addresses[0] != "http://search.internal:9200" {
		t.Fatalf("addresses = %#v", addresses)
	}

	addresses, _, _, err = parseElasticURL("http://a:9200, http://b:9200")
	if err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 2 {
		t.Fatalf("addresses = %#v", addresses)
	}
}
