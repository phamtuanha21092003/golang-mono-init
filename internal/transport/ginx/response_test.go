package ginx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func TestGinSuccessValidationAndNotFound(t *testing.T) {
	server := New(zap.NewNop(), Options{})
	server.GET("/ok", func(c *gin.Context) {
		Success(c, map[string]string{"ok": "1"})
	})
	server.POST("/items", func(c *gin.Context) {
		var body struct {
			Name string `json:"name" validate:"required"`
		}
		if err := Body(c, &body); err != nil {
			Error(c, err)
			return
		}
		Created(c, body)
	})
	server.GET("/items/:id", func(c *gin.Context) {
		var params struct {
			ID string `uri:"id" validate:"required"`
		}
		if err := Params(c, &params); err != nil {
			Error(c, err)
			return
		}
		Success(c, params)
	})
	server.NotFound()

	ok := request(t, server, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if ok.Code != http.StatusOK || ok.Body["success"] != true {
		t.Fatalf("ok = %d %+v", ok.Code, ok.Body)
	}

	missing := request(t, server, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if missing.Code != http.StatusNotFound || missing.Body["error"] != "not_found" {
		t.Fatalf("missing = %d %+v", missing.Code, missing.Body)
	}

	badReq := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(`{}`))
	badReq.Header.Set("Content-Type", "application/json")
	bad := request(t, server, badReq)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("bad = %d %+v", bad.Code, bad.Body)
	}

	item := request(t, server, httptest.NewRequest(http.MethodGet, "/items/7", nil))
	if item.Code != http.StatusOK {
		t.Fatalf("item = %d %+v", item.Code, item.Body)
	}
}

type recorded struct {
	Code int
	Body map[string]any
}

func request(t *testing.T, server *Server, req *http.Request) recorded {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return recorded{Code: recorder.Code, Body: body}
}
