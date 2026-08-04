package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// zenExitReporter posts rejected exits to the control API so the pool cools that
// slot down for the destination. Reporting is advisory: the request that
// triggered it has already moved on to another exit, so every failure here is
// logged and swallowed rather than surfaced.
type zenExitReporter struct {
	controlURL string
	token      string
	cooldown   time.Duration
	client     *http.Client
	logger     *slog.Logger
}

func newZenExitReporter(controlURL, token string, cooldown time.Duration, logger *slog.Logger) *zenExitReporter {
	return &zenExitReporter{
		controlURL: controlURL,
		token:      token,
		cooldown:   cooldown,
		client:     &http.Client{Timeout: 5 * time.Second},
		logger:     logger,
	}
}

// report sends one cooldown request. It never blocks the caller for longer than
// the client timeout and never panics on a failing control plane.
func (r *zenExitReporter) report(slot, target, reason string) {
	payload := map[string]string{
		"slot":     slot,
		"target":   target,
		"reason":   reason,
		"cooldown": r.cooldown.String(),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		r.logger.Debug("encode egress report", slog.String("error_type", fmt.Sprintf("%T", err)))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.controlURL+"/v1/report", bytes.NewReader(body))
	if err != nil {
		r.logger.Debug("build egress report", slog.String("error_type", fmt.Sprintf("%T", err)))
		return
	}
	request.Header.Set("Content-Type", "application/json")
	if r.token != "" {
		request.Header.Set("Authorization", "Bearer "+r.token)
	}

	response, err := r.client.Do(request)
	if err != nil {
		r.logger.Debug("send egress report", slog.String("error_type", fmt.Sprintf("%T", err)))
		return
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			r.logger.Debug("close egress report", slog.String("error_type", fmt.Sprintf("%T", err)))
		}
	}()
	if response.StatusCode != http.StatusOK {
		r.logger.Debug("egress report rejected", slog.Int("status", response.StatusCode))
		return
	}
	r.logger.Info("reported exhausted egress",
		slog.String("slot", slot),
		slog.String("target", target),
		slog.Duration("cooldown", r.cooldown))
}
