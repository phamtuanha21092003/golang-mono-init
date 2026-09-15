package httpx

import (
	"encoding/json"
	"net/http"

	"go-init/internal/base"
)

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type successBody struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
}

// JSON writes a successful response as JSON with the given status code.
func JSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(data)
}

// Success writes { success: true, data: ... } to match NestJS ResponseTransformInterceptor.
func Success(w http.ResponseWriter, data interface{}) {
	JSON(w, http.StatusOK, successBody{
		Success: true,
		Data:    data,
		Message: "success",
	})
}

// Error maps an error (including sentinel/AppError) to a proper HTTP status and writes JSON.
func Error(w http.ResponseWriter, err error) {
	appErr := base.AsAppError(err)
	if appErr == nil {
		JSON(w, http.StatusOK, nil)
		return
	}

	JSON(w, appErr.Code, errorBody{
		Error:   appErr.Message,
		Message: appErr.Error(),
	})
}

type envelope struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
}

func JSONEnvelope(w http.ResponseWriter, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := envelope{
		Success: true,
		Data:    json.RawMessage(data),
	}

	_ = json.NewEncoder(w).Encode(resp)
}
