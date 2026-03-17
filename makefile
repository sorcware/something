.PHONY: dev backend frontend test

ROOT := $(shell pwd)

dev:
	$(MAKE) backend & $(MAKE) frontend; kill %1

backend:
	cd $(ROOT)/backend && $(ROOT)/backend/.venv/bin/uvicorn api:app --reload --reload-dir $(ROOT)/backend

frontend:
	cd frontend && bun run dev

test:
	cd $(ROOT)/backend && uv run pytest