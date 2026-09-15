package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"go-init/internal/bootstrap"
	"go-init/internal/transport/ginx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gin-api: %v\n", err)
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

	server := ginx.New(resources.Log, ginx.Options{
		Name:  resources.Stack.Name,
		Addr:  resources.Stack.ListenAddr(),
		Debug: resources.Stack.Debug,
	})
	server.GET("/ping", func(c *gin.Context) {
		ginx.Success(c, map[string]string{"message": "welcome"})
	})
	server.Health("/health", resources.Checks()...)
	server.NotFound()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Run(ctx)
}
