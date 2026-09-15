package fiberx

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
)

func TestFiberSuccessValidationAndNotFound(t *testing.T) {
	server := New(zap.NewNop(), Options{})
	server.Get("/ok", func(c fiber.Ctx) error {
		return Success(c, map[string]string{"ok": "1"})
	})
	server.Post("/items", func(c fiber.Ctx) error {
		var body struct {
			Name string `json:"name" validate:"required"`
		}
		if err := Body(c, &body); err != nil {
			return Error(c, err)
		}
		return Created(c, body)
	})
	server.NotFound()

	ok := decode(t, mustTest(t, server, httptest.NewRequest(http.MethodGet, "/ok", nil)))
	if ok.StatusCode != http.StatusOK || ok.Body["success"] != true {
		t.Fatalf("ok = %d %+v", ok.StatusCode, ok.Body)
	}

	missing := decode(t, mustTest(t, server, httptest.NewRequest(http.MethodGet, "/missing", nil)))
	if missing.StatusCode != http.StatusNotFound || missing.Body["error"] != "not_found" {
		t.Fatalf("missing = %d %+v", missing.StatusCode, missing.Body)
	}

	badReq := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{}`))
	badReq.Header.Set("Content-Type", "application/json")
	bad := decode(t, mustTest(t, server, badReq))
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad = %d %+v", bad.StatusCode, bad.Body)
	}

	goodReq := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{"name":"widget"}`))
	goodReq.Header.Set("Content-Type", "application/json")
	good := decode(t, mustTest(t, server, goodReq))
	if good.StatusCode != http.StatusCreated {
		t.Fatalf("created = %d %+v", good.StatusCode, good.Body)
	}
}

type recorded struct {
	StatusCode int
	Body       map[string]any
}

func mustTest(t *testing.T, server *Server, req *http.Request) *http.Response {
	t.Helper()
	resp, err := server.Test(req, fiber.TestConfig{Timeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response) recorded {
	t.Helper()
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	return recorded{StatusCode: resp.StatusCode, Body: body}
}
