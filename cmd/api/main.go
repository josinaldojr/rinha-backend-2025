package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/josinaldojr/rinha-backend-2025/internal/config"
	"github.com/josinaldojr/rinha-backend-2025/internal/decider"
	"github.com/josinaldojr/rinha-backend-2025/internal/handlers"
	"github.com/josinaldojr/rinha-backend-2025/internal/processors"
	"github.com/josinaldojr/rinha-backend-2025/internal/repo"
	"github.com/josinaldojr/rinha-backend-2025/internal/server"
)

func main() {
	cfg := config.FromEnv()

	db, err := repo.Open(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close(context.Background())

	proc := processors.NewClient(cfg.PPDefaultURL, cfg.PPFallbackURL)
	d := decider.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	decider.StartHealthWorker(ctx, db, proc, d)

	h := handlers.New(db, proc, d)

	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.RequestID)
	r.Use(middleware.Timeout(5 * time.Second))

	r.Post("/payments", h.CreatePayment)
	r.Get("/payments-summary", h.Summary)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := server.New(r, ":9999")

	go func() {
		log.Printf("listening on %s", ":9999")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	cancel()
	ctx2, c2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer c2()
	_ = srv.Shutdown(ctx2)
}
