package consumer

import (
	"context"
	"testing"

	"go-init/internal/dto"
	"go-init/internal/transport/rabbitmq"
)

type stubCommon struct {
	called bool
}

func (s *stubCommon) Handle(ctx context.Context, env dto.Envelope, msg rabbitmq.Message) error {
	s.called = true
	return nil
}

func TestHandlerForUnknown(t *testing.T) {
	_, err := HandlerFor("does-not-exist", Deps{})
	if err == nil {
		t.Fatal("expected error for unknown handler")
	}
}

func TestHandlerForCommon(t *testing.T) {
	stub := &stubCommon{}
	h, err := HandlerFor("common", Deps{Common: stub})
	if err != nil {
		t.Fatal(err)
	}
	if err := h(context.Background(), rabbitmq.Message{Body: []byte(`{"kind":"ping"}`)}); err != nil {
		t.Fatal(err)
	}
	if !stub.called {
		t.Fatal("expected CommonService.Handle to be called")
	}
}

func TestDecodeEnvelope(t *testing.T) {
	env := decodeEnvelope([]byte(`{"kind":"order.created","payload":{"id":1}}`))
	if env.Kind != "order.created" {
		t.Fatalf("kind: %s", env.Kind)
	}
	if decodeEnvelope([]byte("not-json")).Kind != "" {
		t.Fatal("invalid json should yield empty envelope")
	}
}
