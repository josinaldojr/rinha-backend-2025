package processors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Provider string
const (
	ProviderDefault Provider = "default"
	ProviderFallback Provider = "fallback"
)

type Client struct {
	defaultURL  string
	fallbackURL string
	http        *http.Client
}

func NewClient(defURL, fbURL string) *Client {
	return &Client{
		defaultURL:  defURL,
		fallbackURL: fbURL,
		http: &http.Client{ Timeout: 350 * time.Millisecond },
	}
}

type payReq struct {
	CorrelationID uuid.UUID `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

func (c *Client) Pay(ctx context.Context, provider Provider, id uuid.UUID, amount decimal.Decimal) error {
	body, _ := json.Marshal(payReq{CorrelationID: id, Amount: amount, RequestedAt: time.Now().UTC()})
	url := c.defaultURL
	if provider == ProviderFallback { url = c.fallbackURL }
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/payments", url), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 { return fmt.Errorf("processor status: %d", resp.StatusCode) }
	return nil
}

type HealthInfo struct {
    Failing       bool `json:"failing"`
    MinResponseMs int  `json:"minResponseTime"`
}

func (c *Client) Health(ctx context.Context, provider Provider) (HealthInfo, error) {
    url := c.defaultURL
    if provider == ProviderFallback { url = c.fallbackURL }

    req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
        fmt.Sprintf("%s/payments/service-health", url), nil)

    resp, err := c.http.Do(req)
    if err != nil { return HealthInfo{}, err }
    defer resp.Body.Close()

    if resp.StatusCode/100 != 2 {
        return HealthInfo{}, fmt.Errorf("health status: %d", resp.StatusCode)
    }

    var out HealthInfo
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
        return HealthInfo{}, err
    }
    return out, nil
}