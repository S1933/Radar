package config

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the single configuration entry point for the radar daemon.
type Config struct {
	Timezone string `yaml:"timezone"`

	LogLevel string `yaml:"log_level"`

	Database DatabaseConfig `yaml:"database"`

	Briefing BriefingConfig `yaml:"briefing"`

	Topics []string `yaml:"topics"`

	RSS    RSSConfig    `yaml:"rss"`
	Reddit RedditConfig `yaml:"reddit"`
	GitHub GitHubConfig `yaml:"github"`
	X      XConfig      `yaml:"x"`

	Models ModelsConfig `yaml:"models"`
}

type DatabaseConfig struct {
	// URL takes precedence over individual fields. If empty, it is built
	// from Host/Port/User/Password/Name.
	URL      string `yaml:"url"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
}

// DSN returns the PostgreSQL connection string. Individual fields are
// expanded from environment variables when set: RADAR_DB_HOST, RADAR_DB_PORT,
// RADAR_DB_USER, RADAR_DB_PASSWORD, RADAR_DB_NAME, RADAR_DATABASE_URL.
func (d DatabaseConfig) DSN() string {
	if u := envOr("RADAR_DATABASE_URL", d.URL); u != "" {
		return u
	}
	host := envOr("RADAR_DB_HOST", d.Host)
	user := envOr("RADAR_DB_USER", d.User)
	pass := envOr("RADAR_DB_PASSWORD", d.Password)
	name := envOr("RADAR_DB_NAME", d.Name)
	port := d.Port
	if s := os.Getenv("RADAR_DB_PORT"); s != "" {
		fmt.Sscanf(s, "%d", &port)
	}
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 5432
	}
	if user == "" {
		user = "radar"
	}
	if name == "" {
		name = "radar"
	}
	// url.UserPassword applies the correct percent-encoding for
	// the userinfo component of a URL — '+' is reserved for the
	// query component, so url.QueryEscape (the previous approach)
	// would silently turn an actual space (' ') in a password
	// into a literal '+' that Postgres would not decode. With
	// a generated hex password the bug is invisible; with a
	// passphrase from a password manager it bites.
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + name,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

type BriefingConfig struct {
	Schedule  string   `yaml:"schedule"`  // single "07:00" (legacy)
	Schedules []string `yaml:"schedules"` // multiple daily slots, e.g. ["08:00","14:00","20:00"]
	Timezone  string   `yaml:"timezone"`  // overrides top-level timezone
	MaxItems  int      `yaml:"max_items"`
	MaxTrends int      `yaml:"max_trends"`
}

type RSSConfig struct {
	Enabled bool      `yaml:"enabled"`
	Feeds   []RSSFeed `yaml:"feeds"`
}

type RSSFeed struct {
	Name   string   `yaml:"name"`
	URL    string   `yaml:"url"`
	Topics []string `yaml:"topics"`
}

type RedditConfig struct {
	Enabled    bool          `yaml:"enabled"`
	Subreddits []string      `yaml:"subreddits"`
	Listing    string        `yaml:"listing"` // hot | new | rising | top
	Limit      int           `yaml:"limit"`   // posts per subreddit per poll
	Every      time.Duration `yaml:"every"`   // own poll interval (e.g. 60m); 0 = global collect cycle
}

type GitHubConfig struct {
	Enabled       bool     `yaml:"enabled"`
	Repositories  []string `yaml:"repositories"`
	Organizations []string `yaml:"organizations"`
	Topics        []string `yaml:"topics"`
}

type XConfig struct {
	Enabled     bool          `yaml:"enabled"`
	Accounts    []string      `yaml:"accounts"`
	Queries     []string      `yaml:"queries"`
	Lists       []string      `yaml:"lists"` // X list IDs (x.com/i/lists/<id>) — followed live
	APIKey      string        `yaml:"-"`     // from env X_AUTH_TOKEN
	APISecret   string        `yaml:"-"`     // from env X_CT0
	BearerToken string        `yaml:"-"`     // from env X_BEARER_TOKEN
	Limit       int           `yaml:"limit"`   // tweets per account / query / list
	Timeout     time.Duration `yaml:"timeout"` // sidecar invocation budget
}

type ModelsConfig struct {
	BaseURL string      `yaml:"base_url"` // OpenAI-compatible endpoint
	APIKey  string      `yaml:"api_key"`
	LLMRank bool        `yaml:"llm_rank"` // use LLM for Stage-2 scoring (slow; off by default)
	// RankEngine selects the Stage-2 scorer: "heuristic" (default), "llm" or
	// "jev". Empty keeps the legacy behaviour (llm_rank ? llm : heuristic).
	RankEngine string      `yaml:"rank_engine"`
	Jev        JevConfig   `yaml:"jev"`
	Filter     ModelConfig `yaml:"filter"`
	Rank       ModelConfig `yaml:"rank"`
	Synth      ModelConfig `yaml:"synthesis"`
}

// JevConfig points at the TypeSafe System One endpoint (model "Jev"). One call
// per item returns typed sub-scores, so the Stage-2 ranker is fast enough to
// leave enabled.
type JevConfig struct {
	Enabled bool   `yaml:"enabled"`
	BaseURL string `yaml:"base_url"`
	Model   string `yaml:"model"`
	APIKey  string `yaml:"-"` // from TYPESAFE_API_KEY, never from YAML
	// Timeout covers one item's request; Jev answers in well under a second,
	// so this is a safety net, not a budget.
	Timeout time.Duration `yaml:"timeout"`
	// IncludeThreshold is the Noul value above which the item is considered
	// briefing material. Exposed for the compare command; the live
	// notification gate stays notify.score_threshold.
	IncludeThreshold float64 `yaml:"include_threshold"`
}

type ModelConfig struct {
	Model     string `yaml:"model"`
	MaxTokens int    `yaml:"max_tokens"`
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// LoadConfig reads a YAML config file and returns the parsed Config.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.defaults()
	// Secrets come from the environment (never from YAML), matching the
	// database credential pattern. OPENAI_API_KEY feeds the LLM stage.
	if cfg.Models.APIKey == "" {
		cfg.Models.APIKey = os.Getenv("OPENAI_API_KEY")
	}
	if cfg.Models.BaseURL == "" {
		cfg.Models.BaseURL = os.Getenv("OPENAI_BASE_URL")
	}
	// TypeSafe key for the Jev Stage-2 ranker (env only, like the rest).
	if cfg.Models.Jev.APIKey == "" {
		cfg.Models.Jev.APIKey = os.Getenv("TYPESAFE_API_KEY")
	}
	// X session cookies for the twscrape sidecar (auth_token + ct0).
	if cfg.X.Enabled {
		if v := os.Getenv("X_AUTH_TOKEN"); v != "" {
			cfg.X.APIKey = v // reused as auth_token cookie
		}
		if v := os.Getenv("X_API_SECRET"); v != "" {
			cfg.X.APISecret = v // reserved for future app-level auth
		}
		if v := os.Getenv("X_CT0"); v != "" {
			cfg.X.APISecret = v // ct0 cookie (overrides API_SECRET if both set)
		}
		cfg.X.BearerToken = os.Getenv("X_BEARER_TOKEN")
	}
	return cfg, nil
}

func (c *Config) defaults() {
	if c.Timezone == "" {
		c.Timezone = "Europe/Paris"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.Briefing.Schedule == "" {
		// The legacy default only applies when no modern slot list is
		// configured, otherwise it silently adds itself to the list
		// and produces a fourth briefing nobody asked for.
		if len(c.Briefing.Schedules) == 0 {
			c.Briefing.Schedule = "07:00"
		}
	}
	if c.Briefing.MaxItems == 0 {
		c.Briefing.MaxItems = 10
	}
	if c.Briefing.MaxTrends == 0 {
		c.Briefing.MaxTrends = 3
	}
	if c.Reddit.Listing == "" {
		c.Reddit.Listing = "hot"
	}
	if c.Reddit.Limit == 0 {
		c.Reddit.Limit = 25
	}
	if c.X.Limit == 0 {
		c.X.Limit = 15
	}
	if c.X.Timeout == 0 {
		// 3 minutes is the budget for 5 accounts + 2 queries + 1 list
		// at 15 tweets each, including twscrape's per-request throttling.
		// The previous 60s was not enough on slow days and caused spurious
		// 0-item cycles.
		c.X.Timeout = 3 * time.Minute
	}
	if c.Models.BaseURL == "" {
		c.Models.BaseURL = "https://api.openai.com/v1"
	}
	if c.Models.Jev.BaseURL == "" {
		c.Models.Jev.BaseURL = "https://api.typesafe.ai/v1"
	}
	if c.Models.Jev.Model == "" {
		c.Models.Jev.Model = "jev-latest"
	}
	if c.Models.Jev.Timeout == 0 {
		c.Models.Jev.Timeout = 20 * time.Second
	}
	if c.Models.Jev.IncludeThreshold == 0 {
		c.Models.Jev.IncludeThreshold = 0.6
	}
}

// Location returns the configured timezone.
func (c *Config) Location() *time.Location {
	tz := c.Timezone
	if c.Briefing.Timezone != "" {
		tz = c.Briefing.Timezone
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.UTC
	}
	return loc
}
