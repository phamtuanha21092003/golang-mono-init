package base

import (
	"errors"
	"net/http"
)

// Sentinel errors shared across the repository/service layers.
// Callers should match them with errors.Is instead of comparing strings.
var (
	ErrNotFound     = errors.New("resource not found")
	ErrInvalidInput = errors.New("invalid input")
)

// AppError wraps a business error with an HTTP status code and an identifying message.
type AppError struct {
	Code    int
	Err     error
	Message string
}

func (e *AppError) Error() string {
	if e.Err == nil {
		return e.Message
	}
	return e.Err.Error()
}

// Unwrap lets errors.Is/errors.As reach the underlying error.
func (e *AppError) Unwrap() error {
	return e.Err
}

func newAppError(code int, message string, err error) *AppError {
	return &AppError{Code: code, Message: message, Err: err}
}

func BadRequest(err error) error {
	return newAppError(http.StatusBadRequest, "bad_request", err)
}

func Unauthorized(err error) error {
	return newAppError(http.StatusUnauthorized, "unauthorized", err)
}

func Forbidden(err error) error {
	return newAppError(http.StatusForbidden, "forbidden", err)
}

func NotFound(err error) error {
	return newAppError(http.StatusNotFound, "not_found", err)
}

func Conflict(err error) error {
	return newAppError(http.StatusConflict, "conflict", err)
}

func InternalServerError(err error) error {
	return newAppError(http.StatusInternalServerError, "internal_server_error", err)
}

func GatewayTimeout(err error) error {
	return newAppError(http.StatusGatewayTimeout, "gateway_timeout", err)
}

// AsAppError converts any error into an *AppError so the HTTP layer can map a status code.
// If the error already is an AppError it is returned as-is; ErrNotFound maps to 404,
// ErrInvalidInput maps to 400, and anything else is treated as a 500.
func AsAppError(err error) *AppError {
	if err == nil {
		return nil
	}

	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr
	}

	if errors.Is(err, ErrNotFound) {
		return newAppError(http.StatusNotFound, "not_found", err)
	}

	if errors.Is(err, ErrInvalidInput) {
		return newAppError(http.StatusBadRequest, "bad_request", err)
	}

	return newAppError(http.StatusInternalServerError, "internal_server_error", err)
}
