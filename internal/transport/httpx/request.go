package httpx

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"

	"go-init/internal/base"
)

// validate is the shared validator instance (safe for concurrent use).
var validate = validator.New(validator.WithRequiredStructEnabled())

// Validate runs struct validation using `validate` tags and returns a 400 AppError on failure.
// Invalid/malformed validate tags cause go-playground/validator to panic; those are recovered
// and returned as 500 so the request does not crash the handler.
func Validate(dst any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = base.InternalServerError(errors.Errorf("invalid validate tag: %v", r))
		}
	}()

	if err := validate.Struct(dst); err != nil {
		return base.BadRequest(err)
	}
	return nil
}

// Body decodes the JSON request body into dst (a non-nil pointer) and validates it.
func Body(r *http.Request, dst any) error {
	if r.Body == nil {
		return base.BadRequest(errors.New("empty request body"))
	}

	dec := json.NewDecoder(r.Body)
	// dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid request body"))
	}

	return Validate(dst)
}

// Query binds URL query parameters into dst using the `query` struct tag.
// dst must be a non-nil pointer to a struct. Fields tagged `query:"-"` are skipped.
func Query(r *http.Request, dst any) error {
	if dst == nil {
		return errors.New("query destination is nil")
	}

	v := reflect.ValueOf(dst)

	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("query destination must be a non-nil pointer")
	}

	v = v.Elem()

	if v.Kind() != reflect.Struct {
		return errors.New("query destination must point to struct")
	}

	if err := bindStruct(v, r.URL.Query()); err != nil {
		return err
	}

	return Validate(dst)
}

// Param returns a path parameter by name (e.g. "/products/{id}" -> Param(r, "id")).
func Param(r *http.Request, name string) string {
	return chi.URLParam(r, name)
}

// Params binds path parameters into dst using the `param` struct tag, then validates it.
// dst must be a non-nil pointer to a struct. Fields tagged `param:"-"` are skipped.
func Params(r *http.Request, dst any) error {
	v := reflect.ValueOf(dst)

	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("param destination must be a non-nil pointer")
	}

	v = v.Elem()

	if v.Kind() != reflect.Struct {
		return errors.New("param destination must point to a struct")
	}

	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		tag := field.Tag.Get("param")
		name := strings.Split(tag, ",")[0]

		if name == "" || name == "-" {
			continue
		}

		raw := chi.URLParam(r, name)

		if raw == "" {
			continue
		}

		fieldValue := v.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		if err := setField(fieldValue, []string{raw}); err != nil {
			return errors.New(
				"invalid path param " + name + ": " + err.Error(),
			)
		}
	}

	return Validate(dst)
}

// ParamInt64 returns a path parameter parsed as int64.
func ParamInt64(r *http.Request, name string) (int64, error) {
	raw := Param(r, name)
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, base.BadRequest(errors.Wrapf(err, "invalid path param %q", name))
	}
	return n, nil
}

func bindStruct(v reflect.Value, values url.Values) error {
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		fieldValue := v.Field(i)
		fieldType := t.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		tag := fieldType.Tag.Get("query")

		name := strings.Split(tag, ",")[0]

		// nested struct
		if name == "" && fieldValue.Kind() == reflect.Struct {
			if err := bindStruct(fieldValue, values); err != nil {
				return err
			}

			continue
		}

		if name == "" || name == "-" {
			continue
		}

		rawValues, exists := values[name]

		if !exists {
			continue
		}

		if err := setField(fieldValue, rawValues); err != nil {
			return errors.New(
				"invalid query param: " + name,
			)
		}
	}

	return nil
}

func parseTime(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}

	var err error

	for _, layout := range layouts {
		var t time.Time

		t, err = time.Parse(layout, value)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, err
}

func setField(field reflect.Value, values []string) error {
	// datetime
	if field.Type() == reflect.TypeOf(time.Time{}) {
		v, err := parseTime(values[0])
		if err != nil {
			return err
		}

		field.Set(reflect.ValueOf(v))
		return nil
	}

	// pointer
	if field.Kind() == reflect.Ptr {

		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}

		return setField(field.Elem(), values)
	}

	// slice
	if field.Kind() == reflect.Slice {

		slice := reflect.MakeSlice(
			field.Type(),
			len(values),
			len(values),
		)

		for i, value := range values {
			if err := setField(slice.Index(i), []string{value}); err != nil {
				return err
			}
		}

		field.Set(slice)

		return nil
	}

	value := values[0]

	switch field.Kind() {

	case reflect.String:
		field.SetString(value)

	case reflect.Bool:
		v, err := strconv.ParseBool(value)

		if err != nil {
			return err
		}

		field.SetBool(v)

	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:

		v, err := strconv.ParseInt(
			value,
			10,
			field.Type().Bits(),
		)

		if err != nil {
			return err
		}

		field.SetInt(v)

	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:

		v, err := strconv.ParseUint(
			value,
			10,
			field.Type().Bits(),
		)

		if err != nil {
			return err
		}

		field.SetUint(v)

	case reflect.Float32,
		reflect.Float64:

		v, err := strconv.ParseFloat(
			value,
			field.Type().Bits(),
		)

		if err != nil {
			return err
		}

		field.SetFloat(v)

	default:
		return errors.New(
			"unsupported type " + field.Type().String(),
		)
	}

	return nil
}
