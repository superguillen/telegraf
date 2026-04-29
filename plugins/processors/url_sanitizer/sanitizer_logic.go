package url_sanitizer

import (
	"strings"
)

// classify evaluates the uri against configured extension, prefix, and contains rules.
func (u *UrlSanitizer) classify(uri string) string {
	if uri == "" {
		return "unknown"
	}

	// 1. Fast-Path for Strict Static Assets (overrides API rules)
	// This protects files like /v2/app.js from being classified as api_rest
	if idx := strings.LastIndexByte(uri, '.'); idx != -1 {
		ext := uri[idx:]
		if cat, exists := staticExtCategories[ext]; exists {
			// Do not fast-path ambiguous extensions like .json, .xml, .yaml
			if cat != "data" {
				return "static_asset"
			}
		}
	}

	// 2. Prefix Rules (most specific — protects /api/ from being overridden by .json ext)
	for prefix, uriType := range u.PreRules {
		if strings.HasPrefix(uri, prefix) {
			return uriType
		}
	}

	// 3. Contains Rules
	for contain, uriType := range u.ConRules {
		if strings.Contains(uri, contain) {
			return uriType
		}
	}

	// 4. Extension classify (from ExtClassify — least specific, fallback for URIs without known prefixes)
	if idx := strings.LastIndexByte(uri, '.'); idx != -1 {
		ext := uri[idx:]
		if uriType, exists := u.ExtClassify[ext]; exists {
			return uriType
		}
	}

	return "unknown"
}

// sanitize takes a raw URI, returns the sanitized URI replacing dynamic segments (IDs, UUIDs, Hashes).
func sanitize(uri string, isStatic bool) string {
	if !strings.Contains(uri, "/") {
		return uri
	}

	parts := strings.Split(uri, "/")
	modified := false

	for i, part := range parts {
		if part == "" {
			continue
		}

		if isTemplateVar(part) {
			parts[i] = "{var}"
			modified = true
			continue
		}

		if isUUID(part) {
			parts[i] = "{uuid}"
			modified = true
			continue
		}

		if isDate(part) {
			parts[i] = "{date}"
			modified = true
			continue
		}

		if isNumeric(part) {
			parts[i] = "{id}"
			modified = true
			continue
		}

		if isFloatOrCoord(part) {
			parts[i] = "{coord}"
			modified = true
			continue
		}

		if strings.Contains(part, ".") {
			ext := part[strings.LastIndexByte(part, '.'):]
			if isStatic {
				if cat, ok := staticExtCategories[ext]; ok {
					parts[i] = "{" + cat + "}" + ext
					modified = true
					continue
				}
			}
			subParts := strings.Split(part, ".")
			subModified := false
			for j, subPart := range subParts {
				if isHash(subPart) {
					subParts[j] = "{hash}"
					subModified = true
				}
			}
			if subModified {
				parts[i] = strings.Join(subParts, ".")
				modified = true
				continue
			}
		}

		if isHash(part) {
			parts[i] = "{hash}"
			modified = true
			continue
		}

		if isULIDOrSimilar(part) {
			parts[i] = "{id}"
			modified = true
			continue
		}

		if isAlphaNumSuffixedID(part) {
			parts[i] = "{id}"
			modified = true
			continue
		}

		if isPrefixedID(part) {
			parts[i] = "{id}"
			modified = true
			continue
		}

		if isEmail(part) {
			parts[i] = "{email}"
			modified = true
			continue
		}

		if isIPv4(part) {
			parts[i] = "{ip}"
			modified = true
			continue
		}
	}

	if !modified {
		return uri
	}

	return strings.Join(parts, "/")
}

// sanitizeQuery takes a query string (without the leading '?') and replaces all values with '{val}'.
// Example: "id=123&sort=asc" -> "?id={val}&sort={val}"
func sanitizeQuery(query string) string {
	if query == "" {
		return ""
	}
	parts := strings.Split(query, "&")
	for i, p := range parts {
		if eqIdx := strings.IndexByte(p, '='); eqIdx != -1 {
			parts[i] = p[:eqIdx] + "={val}"
		}
	}
	return "?" + strings.Join(parts, "&")
}

var staticExtCategories = map[string]string{
	".jpg": "image", ".jpeg": "image", ".png": "image", ".gif": "image",
	".webp": "image", ".svg": "image", ".ico": "image", ".bmp": "image",
	".tiff": "image", ".tif": "image", ".avif": "image",
	".heic": "image", ".heif": "image", ".raw": "image", ".psd": "image", ".eps": "image",
	".css": "stylesheet", ".scss": "stylesheet", ".less": "stylesheet", ".sass": "stylesheet",
	".js": "script", ".mjs": "script", ".ts": "script", ".tsx": "script", ".jsx": "script", ".cjs": "script",
	".woff": "font", ".woff2": "font", ".ttf": "font", ".otf": "font", ".eot": "font",
	".pdf": "document", ".doc": "document", ".docx": "document",
	".xls": "document", ".xlsx": "document", ".csv": "document",
	".ppt": "document", ".pptx": "document", ".rtf": "document", ".odt": "document", ".txt": "document",
	".mp3": "audio", ".wav": "audio", ".ogg": "audio", ".flac": "audio", ".aac": "audio", ".m4a": "audio",
	".wma": "audio", ".opus": "audio", ".mid": "audio", ".midi": "audio",
	".mp4": "video", ".webm": "video", ".avi": "video", ".mov": "video", ".mkv": "video", ".flv": "video", ".wmv": "video",
	".m4v": "video", ".3gp": "video",
	".json": "data", ".xml": "data", ".yaml": "data", ".yml": "data", ".toml": "data",
	".zip": "archive", ".gz": "archive", ".tar": "archive", ".rar": "archive", ".7z": "archive",
	".bz2": "archive", ".xz": "archive", ".tgz": "archive",
	".html": "page", ".htm": "page", ".xhtml": "page",
	".map":         "sourcemap",
	".webmanifest": "manifest", ".appcache": "manifest",
	".wasm": "binary",
}

// isFloatOrCoord detects strings representing floating point numbers or coordinates.
// E.g., 19.3064068 or -99.1873094
func isFloatOrCoord(s string) bool {
	if len(s) < 3 {
		return false
	}
	hasDot := false
	hasDigit := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 0 && c == '-' {
			continue
		}
		if c == '.' {
			if hasDot {
				return false // more than one dot
			}
			hasDot = true
			continue
		}
		if c >= '0' && c <= '9' {
			hasDigit = true
			continue
		}
		return false
	}
	return hasDot && hasDigit
}

// isTemplateVar detects strings enclosed in `%`
func isTemplateVar(s string) bool {
	l := len(s)
	if l > 2 && s[0] == '%' && s[l-1] == '%' {
		return true
	}
	return false
}

// Helpers for zero-regex parsing

// isUUID checks for standard 36-char 8-4-4-4-12 hex format
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	return isHex(s[:8]) && isHex(s[9:13]) && isHex(s[14:18]) && isHex(s[19:23]) && isHex(s[24:])
}

// isDate checks for YYYY-MM-DD
func isDate(s string) bool {
	if len(s) != 10 {
		return false
	}
	if s[4] != '-' || s[7] != '-' {
		return false
	}
	return isNumeric(s[:4]) && isNumeric(s[5:7]) && isNumeric(s[8:])
}

// isNumeric checks if all characters are digits
func isNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isHex checks if all characters are hexadecimal
func isHex(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// isHash checks if the string is a pure hexadecimal hash of common lengths
func isHash(s string) bool {
	l := len(s)
	// Common hash lengths: 24 (Mongo ObjectId), 32 (MD5), 40 (SHA1), 64 (SHA256)
	if l == 24 || l == 32 || l == 40 || l == 64 {
		return isHex(s)
	}
	// For static assets like main.a1b2c3d4.css, they might have 8 char hashes.
	if l == 8 {
		return isHex(s)
	}
	return false
}

// isULIDOrSimilar checks for high-entropy alphanumeric strings common in modern DBs
// ULIDs are 26 chars Base32. KSUIDs are 27 chars Base62. NanoIDs and CUIDs are variable (often ~21-25).
// For performance, we check if the string is between 20 and 30 characters and is fully alphanumeric,
// avoiding false positives with normal path words (which are rarely 20+ chars long without hyphens).
func isULIDOrSimilar(s string) bool {
	l := len(s)
	// We want strictly upper and lower case letters + digits. No slashes, hyphens or periods.
	// Many normal application paths can be long words (e.g. ApplicationInitializer)
	// High entropy IDs usually contain numbers and uppercase and lowercase mixed, but for speed
	// we just ensure length is 21-27 (the standard bounds) and it's pure alnum.
	// But to avoid replacing "ApplicationInitializer" (length 22) we can require at least one digit
	// since pure alphabet words of length 22 exist but IDs almost always have digits.
	if l < 21 || l > 27 {
		return false
	}
	hasDigit := false
	for i := 0; i < l; i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			hasDigit = true
		} else if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}
	return hasDigit
}

// isAlphaNumSuffixedID detects segments of the form [letters][digits][optional-letters],
// e.g. content38291047MOV or report20240101PDF.
// Requires a contiguous digit run of at least 5 to avoid version strings like "v2" or "chapter3".
func isAlphaNumSuffixedID(s string) bool {
	l := len(s)
	if l < 8 {
		return false
	}
	for i := 0; i < l; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}
	maxRun := 0
	run := 0
	for i := 0; i < l; i++ {
		if s[i] >= '0' && s[i] <= '9' {
			run++
			if run > maxRun {
				maxRun = run
			}
		} else {
			run = 0
		}
	}
	return maxRun >= 4
}

// isPrefixedID checks for "Stripe-like" pattern: short_prefix_followedByHighEntropy
// Also handles UUIDs with a prefix like rb_c2de862c-f709-4825-ab76-ee9db08f5ca3
func isPrefixedID(s string) bool {
	// Look for the first underscore
	idx := strings.IndexByte(s, '_')
	if idx >= 2 && idx <= 5 { // prefix is 2-5 chars
		// Right part must be alphanumeric and high entropy (e.g. 10 to 36 chars)
		right := s[idx+1:]
		if len(right) >= 10 && len(right) <= 36 { // 36 to fit a UUID
			return isAlphanumericOrHyphen(right)
		}
	}
	return false
}

// isAlphanumericOrHyphen checks if all characters are letters, digits, or hyphens
func isAlphanumericOrHyphen(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

// isEmail performs a fast zero-regex basic validation for email shapes
func isEmail(s string) bool {
	idxAt := strings.IndexByte(s, '@')
	if idxAt < 1 || idxAt == len(s)-1 {
		return false
	}
	idxDot := strings.LastIndexByte(s, '.')
	// Dot must be after the @ and not at the very end
	if idxDot < idxAt+2 || idxDot == len(s)-1 {
		return false
	}
	return true
}

// isIPv4 performs a fast check for IPv4 structures (e.g., 192.168.1.1)
func isIPv4(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if !isNumeric(p) || len(p) > 3 || len(p) == 0 {
			return false
		}
	}
	return true
}

// isAlphanumeric checks if all characters are letters or digits
func isAlphanumeric(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}
