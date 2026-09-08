package briefing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/textutil"
)

// synthesizer calls an OpenAI-compatible chat endpoint to generate a short
// "why it matters" rationale per item. Used as an optional enrichment when
// models.base_url + models.api_key are configured.
type synthesizer struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func newSynthesizer(cfg config.ModelsConfig) *synthesizer {
	return &synthesizer{
		baseURL: cfg.BaseURL,
		apiKey:  cfg.APIKey,
		model:   cfg.Synth.Model,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// Rationale asks the LLM for a one-line "why this matters" for an item.
func (s *synthesizer) Rationale(ctx context.Context, title, content, source string) (string, error) {
	prompt := fmt.Sprintf(
		"Source: %s\nTitre: %s\nExtrait: %s\n\nEn une seule phrase concise (max 140 caractères), en français, explique pourquoi cet item intéresse un ingénieur backend senior passionné par les agents IA, Go et l'outillage développeur.",
		source, title, textutil.Truncate(content, 600, "…"))

	body, err := json.Marshal(map[string]any{
		"model": s.model,
		"messages": []map[string]string{
			{"role": "system", "content": "Tu es un éditeur de veille tech percutant. Réponds toujours en français, de façon concise, sans marketing, sans fluff."},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.3,
		"max_tokens":  300,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.apiKey)
	}
	// The OpenCode Go endpoint requires a session header to route
	// requests (400 MissingSessionID otherwise). A stable value is
	// enough — it identifies the client, not a per-user session.
	req.Header.Set("x-opencode-session", "personal-radar")

	res, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 256))
		return "", fmt.Errorf("synthesis: HTTP %d %s", res.StatusCode, string(b))
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return "", err
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("synthesis: empty choices")
	}
	result := parsed.Choices[0].Message.Content
	// Reasoning models (deepseek-v4-flash, glm) stream their draft
	// into reasoning_content and the actual answer into content.
	// The reasoning draft is never user-facing: if content is empty
	// (budget exhausted mid-thought), drop the line entirely rather
	// than leaking the model's inner monologue into the briefing.
	if result == "" {
		return "", nil
	}
	return cleanRationale(result), nil
}

func cleanRationale(s string) string {
	s = stripQuotes(s)
	s = trimNewlines(s)
	// Cap at 160 runes (not bytes): the previous s[:157] + "..." cut a
	// French accent in half and produced U+FFFD in the dashboard.
	s = textutil.Truncate(s, 160, "…")
	return s
}

func stripQuotes(s string) string {
	s = trimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\n' || s[start] == '	') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\n' || s[end-1] == '	') {
		end--
	}
	return s[start:end]
}

func trimNewlines(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' || s[i] == '\r' {
			if len(out) > 0 && out[len(out)-1] == ' ' {
				continue
			}
			out = append(out, ' ')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
