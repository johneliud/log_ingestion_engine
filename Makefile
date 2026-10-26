TAILWIND_VERSION := v4.3.3
TAILWIND_BINARY := bin/tailwindcss-linux-x64

# Regenerates the committed dashboard stylesheet.
dashboard-css: $(TAILWIND_BINARY)
	$(TAILWIND_BINARY) --input internal/dashboard/static/css/input.css --output internal/dashboard/static/css/styles.css --minify

$(TAILWIND_BINARY):
	mkdir -p bin
	curl -fSL -o $(TAILWIND_BINARY) https://github.com/tailwindlabs/tailwindcss/releases/download/$(TAILWIND_VERSION)/tailwindcss-linux-x64
	chmod +x $(TAILWIND_BINARY)

.PHONY: dashboard-css
