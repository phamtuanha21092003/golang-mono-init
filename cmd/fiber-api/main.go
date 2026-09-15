package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"

	"go-init/internal/bootstrap"
	"go-init/internal/transport/fiberx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fiber-api: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()

	resources, err := bootstrap.Open()
	if err != nil {
		return err
	}
	defer resources.Close()

	server := fiberx.New(resources.Log, fiberx.Options{
		Name: resources.Stack.Name,
		Addr: resources.Stack.ListenAddr(),
	})
	server.Get("/ping", func(c fiber.Ctx) error {
		return fiberx.Success(c, map[string]string{"message": "welcome"})
	})
	server.Health("/health", resources.Checks()...)
	server.NotFound()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Run(ctx)
}
