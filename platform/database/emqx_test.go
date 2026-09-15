package database

import "testing"

func TestParseMQTT(t *testing.T) {
	broker, user, pass, err := parseMQTT("mqtt://emqx:secret@broker.internal:1883")
	if err != nil {
		t.Fatal(err)
	}
	if broker != "tcp://broker.internal:1883" {
		t.Fatalf("broker = %s", broker)
	}
	if user != "emqx" || pass != "secret" {
		t.Fatalf("auth = %s %s", user, pass)
	}

	broker, _, _, err = parseMQTT("mqtts://broker.internal:8883")
	if err != nil {
		t.Fatal(err)
	}
	if broker != "ssl://broker.internal:8883" {
		t.Fatalf("broker = %s", broker)
	}
}
