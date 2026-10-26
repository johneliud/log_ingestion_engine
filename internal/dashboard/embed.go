package dashboard

import "embed"

// Assets holds the embedded home, dashboard and metrics pages and the diagram source at the package root, plus the stylesheet, scripts, fonts and vendored libraries under static/.
//
//go:embed index.html root.html metrics.html architecture.mmd static
var Assets embed.FS
