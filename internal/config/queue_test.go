package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestParseQueues(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	raw := `
[[queues]]
name = "common.queue"
exchange = "common.exchange"
binding_key = "common.routing-key"
exchange_kind = "direct"
handler = "common"
max_priority = 10

[[queues]]
name = "jobs.queue"
handler = "jobs"
`
	if err := v.ReadConfig(strings.NewReader(raw)); err != nil {
		t.Fatal(err)
	}

	queues, err := ParseQueues(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 2 {
		t.Fatalf("got %d queues", len(queues))
	}
	if queues[0].Name != "common.queue" || queues[0].Handler != "common" || queues[0].MaxPriority != 10 {
		t.Fatalf("first queue: %+v", queues[0])
	}
	if queues[1].BindingKey != "jobs.queue" {
		t.Fatalf("default binding_key: %s", queues[1].BindingKey)
	}
	if queues[1].ExchangeKind != "direct" {
		t.Fatalf("default exchange_kind: %s", queues[1].ExchangeKind)
	}
}

func TestParseQueuesRequiresNameAndHandler(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	if err := v.ReadConfig(strings.NewReader(`
[[queues]]
exchange = "x"
`)); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseQueues(v); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseQueuesRequiresAtLeastOne(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	if err := v.ReadConfig(strings.NewReader(`# empty`)); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseQueues(v); err == nil {
		t.Fatal("expected error")
	}
}
