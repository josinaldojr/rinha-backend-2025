package config

import (
	"log"
	"os"
)

type Config struct {
	DatabaseURL   string
	PPDefaultURL  string
	PPFallbackURL string
	InstanceID    string
}

func FromEnv() Config {
	cfg := Config{
		DatabaseURL:   getenv("DATABASE_URL", "postgres://rinha:rinha@postgres:5432/rinha?sslmode=disable"),
		PPDefaultURL:  getenv("PP_DEFAULT_URL", "http://payment-processor-default:8080"),
		PPFallbackURL: getenv("PP_FALLBACK_URL", "http://payment-processor-fallback:8080"),
		InstanceID:   getenv("INSTANCE_ID", "0"),
	}
	if cfg.DatabaseURL == "" { log.Fatal("DATABASE_URL required") }
	return cfg
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" { return v }
	return def
}
