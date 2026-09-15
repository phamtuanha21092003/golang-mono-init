package ginx

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"go-init/internal/base"
)

type Options struct {
	Name             string
	Addr             string
	Debug            bool
	AllowOrigins     string
	AllowMethods     string
	AllowHeaders     string
	AllowCredentials bool
}

type Server struct {
	*gin.Engine
	log  *zap.Logger
	addr string
}

func New(log *zap.Logger, opts Options) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	if opts.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	if opts.Addr == "" {
		opts.Addr = ":3000"
	}

	engine := gin.New()
	engine.Use(requestID())
	engine.Use(recoverJSON(log))
	engine.Use(corsHandler(opts))
	engine.Use(accessLog(log))

	return &Server{Engine: engine, log: log, addr: opts.Addr}
}

func (s *Server) Health(path string, checks ...func(context.Context) error) {
	s.GET(path, func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		for _, check := range checks {
			if check == nil {
				continue
			}
			if err := check(ctx); err != nil {
				Error(c, base.ServiceUnavailable(err))
				return
			}
		}
		Success(c, map[string]string{"status": "ok"})
	})
}

// NotFound writes the JSON 404 body. Register it after every route.
func (s *Server) NotFound() {
	s.NoRoute(func(c *gin.Context) {
		Error(c, base.NotFound(errors.New("page not found")))
	})
}

func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.Engine,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
