package ginx

import (
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"

	"go-init/internal/base"
)

func Body(c *gin.Context, dst any) error {
	if err := c.ShouldBindJSON(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid request body"))
	}
	return base.Validate(dst)
}

func Query(c *gin.Context, dst any) error {
	if err := c.ShouldBindQuery(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid query"))
	}
	return base.Validate(dst)
}

func Params(c *gin.Context, dst any) error {
	if err := c.ShouldBindUri(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid path params"))
	}
	return base.Validate(dst)
}

func Param(c *gin.Context, name string) string {
	return c.Param(name)
}
