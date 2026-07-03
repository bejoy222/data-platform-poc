.PHONY: help setup up down restart logs lint test clean

# Default target
help:
	@echo ""
	@echo "Data Platform POC - Available Commands"
	@echo "======================================="
	@echo "make setup     - First-time setup of the platform"
	@echo "make up        - Start all services"
	@echo "make down      - Stop all services"
	@echo "make restart   - Restart all services"
	@echo "make logs      - Tail logs from all services"
	@echo "make lint      - Run all linters"
	@echo "make test      - Run all tests"
	@echo "make clean     - Remove all containers and volumes"
	@echo ""

# First-time setup
setup:
	@echo "Setting up Data Platform POC..."
	@cp -n .env.example .env || true
	@echo "Please edit .env with your values before running make up"

# Start all services
up:
	@echo "Starting all services..."
	docker compose -f deploy/docker-compose/docker-compose.core.yml up -d
	@echo "Services started."

# Stop all services
down:
	@echo "Stopping all services..."
	docker compose -f deploy/docker-compose/docker-compose.core.yml down
	@echo "Services stopped."

# Restart all services
restart: down up

# Tail logs
logs:
	docker compose -f deploy/docker-compose/docker-compose.core.yml logs -f

# Run linters
lint:
	@echo "Running linters..."
	@find . -name "*.py" | xargs python3 -m ruff check || true
	@find . -name "*.yml" -o -name "*.yaml" | xargs python3 -m yamllint || true
	@echo "Linting complete."

# Run tests
test:
	@echo "Running tests..."
	@python3 -m pytest services/ -v || true
	@echo "Tests complete."

# Clean everything
clean:
	@echo "Cleaning up..."
	docker compose -f deploy/docker-compose/docker-compose.core.yml down -v
	@echo "Clean complete."
