package base

import (
	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
)

// validate is safe for concurrent use.
var validate = validator.New(validator.WithRequiredStructEnabled())

// Validate checks `validate` tags. A bad tag panics inside the library; that panic
// becomes a 500 so one malformed struct does not crash the process.
func Validate(dst any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = InternalServerError(errors.Errorf("invalid validate tag: %v", recovered))
		}
	}()

	if err := validate.Struct(dst); err != nil {
		return BadRequest(err)
	}
	return nil
}
