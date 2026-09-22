// Package web embeds the operator dashboard into the binary.
//
// Embedding rather than serving from disk means the binary is the whole
// deployment: no sidecar, no volume mount, no version skew between the API and
// the UI that calls it.
package web

import _ "embed"

// DashboardHTML is the single-page operator dashboard.
//
//go:embed dashboard.html
var DashboardHTML string
