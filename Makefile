SHELL := /bin/bash
FRONTEND_DIR := /srv/crawl-dashboard
COMPOSE := docker compose

.PHONY: help up down logs build deploy-frontend reload-caddy install rotate-key

help:
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## build the backend image
	$(COMPOSE) build backend

up: ## start the stack (tunnel + backend) in the background
	$(COMPOSE) up -d

down: ## stop the stack
	$(COMPOSE) down

logs: ## tail backend logs
	$(COMPOSE) logs -f --tail=200 backend

deploy-frontend: ## sync the static frontend to /srv/crawl-dashboard
	sudo install -d -o caddy -g caddy -m 0755 $(FRONTEND_DIR)
	sudo rsync -a --delete frontend/ $(FRONTEND_DIR)/
	sudo chown -R caddy:caddy $(FRONTEND_DIR)

reload-caddy: ## reload caddy after editing the Caddyfile
	sudo caddy validate --config /etc/caddy/Caddyfile
	sudo systemctl reload caddy

install: build deploy-frontend up reload-caddy ## one-shot bootstrap

rotate-key: ## generate a new API key, print it, restart the backend
	@new=$$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))'); \
	  echo "API_KEY=$$new"; \
	  sed -i "s|^API_KEY=.*|API_KEY=$$new|" .env; \
	  $(COMPOSE) up -d --no-deps backend
