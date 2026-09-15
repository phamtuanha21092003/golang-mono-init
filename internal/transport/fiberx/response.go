package fiberx

import (
	"github.com/gofiber/fiber/v3"

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

func Success(c fiber.Ctx, data any) error {
	return c.Status(fiber.StatusOK).JSON(successBody{
		Success: true,
		Data:    data,
		Message: "success",
	})
}

func Created(c fiber.Ctx, data any) error {
	return c.Status(fiber.StatusCreated).JSON(successBody{
		Success: true,
		Data:    data,
		Message: "success",
	})
}

func Error(c fiber.Ctx, err error) error {
	if err == nil {
		return Success(c, nil)
	}
	appErr := base.AsAppError(err)
	return c.Status(appErr.Code).JSON(errorBody{
		Error:   appErr.Message,
		Message: appErr.Error(),
	})
}

func Paging(c fiber.Ctx, data any, total int64, paging base.PagingInput) error {
	paging.Normalize()
	return Success(c, &base.PagingResponseDto{
		Data:        data,
		Count:       total,
		PageNum:     paging.PageNum,
		PageSize:    paging.PageSize,
		HasNextPage: paging.PageNum*paging.PageSize < total,
		HasPrevPage: paging.PageNum > 1,
	})
}
