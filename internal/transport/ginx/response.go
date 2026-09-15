package ginx

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"go-init/internal/base"
)

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type successBody struct {
	Success bool   `json:"success"`
	Data    any    `json:"data"`
	Message string `json:"message,omitempty"`
}

func Success(c *gin.Context, data any) {
	c.JSON(http.StatusOK, successBody{
		Success: true,
		Data:    data,
		Message: "success",
	})
}

func Created(c *gin.Context, data any) {
	c.JSON(http.StatusCreated, successBody{
		Success: true,
		Data:    data,
		Message: "success",
	})
}

func Error(c *gin.Context, err error) {
	if err == nil {
		Success(c, nil)
		return
	}
	appErr := base.AsAppError(err)
	c.AbortWithStatusJSON(appErr.Code, errorBody{
		Error:   appErr.Message,
		Message: appErr.Error(),
	})
}

func Paging(c *gin.Context, data any, total int64, paging base.PagingInput) {
	paging.Normalize()
	Success(c, &base.PagingResponseDto{
		Data:        data,
		Count:       total,
		PageNum:     paging.PageNum,
		PageSize:    paging.PageSize,
		HasNextPage: paging.PageNum*paging.PageSize < total,
		HasPrevPage: paging.PageNum > 1,
	})
}
