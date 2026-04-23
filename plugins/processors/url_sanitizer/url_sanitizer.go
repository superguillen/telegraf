package url_sanitizer

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

type UrlSanitizer struct {
	Key                 string            `toml:"key"`
	ResultKey           string            `toml:"result_key"`
	TagKey              string            `toml:"tag_key"`
	SanitizeStaticFiles bool              `toml:"sanitize_static_files"`
	ExtClassify         map[string]string `toml:"extension_classify"`
	PreRules            map[string]string `toml:"prefix_rules"`
	ConRules            map[string]string `toml:"contains_rules"`
	StaticCategories    map[string]string `toml:"static_categories"`
}

var sampleConfig = `
  ## Field containing the URI to sanitize.
  key = "URI"
  
  ## Field to store the sanitized URI. If empty, it overwrites key.
  # result_key = "URI_sanitized"
  
  ## Tag to store the URI classification type.
  tag_key = "uri_type"

  ## Sanitize static file names by replacing them with category placeholders (e.g. caballo.jpg -> {image}.jpg).
  ## Only applies when classify() returns "static_asset" to protect API endpoints with ambiguous extensions (.json, .xml).
  sanitize_static_files = true

  ## Optional custom classification rules. These merge with or override default industry standards.
  # [processors.url_sanitizer.extension_classify]
  #   ".asp" = "legacy_api"
  #   ".php" = "server_rendered"

  # [processors.url_sanitizer.prefix_rules]
  #   "/custom-api/" = "legacy_api"

  # [processors.url_sanitizer.contains_rules]
  #   "/v1/" = "api_rest"

  ## Optional custom static file categories. These merge with or override default categories.
  ## Controls the placeholder name used when sanitizing static file names.
  ## Only used when classify() returns "static_asset" and sanitize_static_files = true.
  # [processors.url_sanitizer.static_categories]
  #   ".custom" = "my_asset"
`

func (u *UrlSanitizer) SampleConfig() string {
	return sampleConfig
}

func (u *UrlSanitizer) Description() string {
	return "Sanitizes URLs by replacing dynamic segments (IDs, UUIDs, etc.) and classifies them using configurable rules."
}

func (u *UrlSanitizer) Init() error {
	if u.ExtClassify == nil {
		u.ExtClassify = make(map[string]string)
	}
	for k := range staticExtCategories {
		if _, exists := u.ExtClassify[k]; !exists {
			u.ExtClassify[k] = "static_asset"
		}
	}

	if u.StaticCategories == nil {
		u.StaticCategories = make(map[string]string)
	}
	for k, v := range staticExtCategories {
		if _, exists := u.StaticCategories[k]; !exists {
			u.StaticCategories[k] = v
		}
	}

	if u.PreRules == nil {
		u.PreRules = make(map[string]string)
	}
	defaultPres := map[string]string{
		"/static/": "static_asset", "/_next/": "static_asset",
		"/api/": "api_rest", "/api.": "api_rest",
		"/graphql": "api_graphql", "/query": "api_graphql",
		"/ws/": "websocket", "/wsservice/": "websocket", "/socket.io/": "websocket",
		"/oauth/": "auth", "/auth/": "auth", "/login": "auth", "/sso/": "auth",
		"/webhook/": "webhook", "/hook/": "webhook",
		"/health": "telemetry", "/metrics": "telemetry", "/ping": "telemetry", "/ready": "telemetry",
		"/rpc": "rpc", "/grpc": "rpc", "/soap": "rpc",
	}
	for k, v := range defaultPres {
		if _, exists := u.PreRules[k]; !exists {
			u.PreRules[k] = v
		}
	}

	// Initialize default Contains rules if none provided
	if u.ConRules == nil {
		u.ConRules = make(map[string]string)
	}
	defaultCons := map[string]string{
		"/v1/": "api_rest", "/v2/": "api_rest", "/v3/": "api_rest",
		"/wsservice/": "websocket",
	}
	for k, v := range defaultCons {
		if _, exists := u.ConRules[k]; !exists {
			u.ConRules[k] = v
		}
	}

	return nil
}

func (u *UrlSanitizer) Apply(in ...telegraf.Metric) []telegraf.Metric {
	if u.Key == "" {
		u.Key = "URI"
	}
	if u.ResultKey == "" {
		u.ResultKey = u.Key
	}
	if u.TagKey == "" {
		u.TagKey = "uri_type"
	}

	for _, metric := range in {
		if rawURI, ok := metric.GetField(u.Key); ok {
			if uriStr, ok := rawURI.(string); ok {
				uriType := u.classify(uriStr)
				isStatic := u.SanitizeStaticFiles && uriType == "static_asset"
				sanitizedURI := sanitize(uriStr, isStatic)

				metric.AddField(u.ResultKey, sanitizedURI)
				metric.AddTag(u.TagKey, uriType)
			}
		}
	}
	return in
}

func init() {
	processors.Add("url_sanitizer", func() telegraf.Processor {
		return &UrlSanitizer{
			Key:    "URI",
			TagKey: "uri_type",
		}
	})
}
