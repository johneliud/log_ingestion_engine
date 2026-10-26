package dashboard

import "embed"

// Assets holds the embedded home and dashboard pages and diagram source at the package root, plus the stylesheet, scripts and vendored libraries under static/.
//
//go:embed index.html root.html architecture.mmd static
var Assets embed.FS
