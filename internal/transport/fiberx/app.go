package fiberx

import (
	"context"
	"errors"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"go.uber.org/zap"

	"go-init/internal/base"
)

type Options struct {
	Name             string
	Addr             string
	BodyLimit        int
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	AllowOrigins     string
	AllowMethods     string
	AllowHeaders     string
	AllowCredentials bool
}

type Server struct {
	*fiber.App
	log  *zap.Logger
	addr string
}

func New(log *zap.Logger, opts Options) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	if opts.Name == "" {
		opts.Name = "go-init"
	}
	if opts.Addr == "" {
		opts.Addr = ":3000"
	}
	if opts.BodyLimit == 0 {
		opts.BodyLimit = 4 * 1024 * 1024
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 15 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 30 * time.Second
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = 60 * time.Second
	}

	app := fiber.New(fiber.Config{
		AppName:      opts.Name,
		BodyLimit:    opts.BodyLimit,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		IdleTimeout:  opts.IdleTimeout,
		ErrorHandler: errorHandler(log),
	})
	app.Use(recover.New())
	app.Use(requestID())
	app.Use(corsHandler(opts))
	app.Use(accessLog(log))

	return &Server{App: app, log: log, addr: opts.Addr}
}

func (s *Server) Health(path string, checks ...func(context.Context) error) {
	s.Get(path, func(c fiber.Ctx) error {
		parent := c.Context()
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		defer cancel()
		for _, check := range checks {
			if check == nil {
				continue
			}
			if err := check(ctx); err != nil {
				return Error(c, base.ServiceUnavailable(err))
			}
		}
		return Success(c, map[string]string{"status": "ok"})
	})
}

// NotFound writes the JSON 404 body. Register it after every route.
func (s *Server) NotFound() {
	s.Use(func(c fiber.Ctx) error {
		return Error(c, base.NotFound(errors.New("page not found")))
	})
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Listen(s.addr, fiber.ListenConfig{DisableStartupMessage: true})
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.ShutdownWithContext(shutdownCtx)
	}
}
