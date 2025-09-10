#!/usr/bin/env bash
# Simple helper to run a local PostgreSQL for testing with Podman.
# Usage: ./scripts/podman-postgres.sh start|stop|rm
# Data is persisted under ./tmp/postgres

set -euo pipefail

NAME="globs-postgres"
DATA_DIR="$(pwd)/tmp/postgres"
IMAGE="docker.io/library/postgres:16"
USER_NAME="admin"
PASSWORD="DevTeam!"
PORT="5432"

mkdir -p "${DATA_DIR}"

cmd=${1:-start}

if [[ "${cmd}" == "start" ]]; then
  echo "Starting ${NAME} on port ${PORT}..."
  podman run -d --name ${NAME} \
    -v "${DATA_DIR}:/var/lib/postgresql/data:Z" \
    -e POSTGRES_USER=${USER_NAME} \
    -e POSTGRES_PASSWORD=${PASSWORD} \
    -p ${PORT}:5432 \
    ${IMAGE}
  echo "Postgres is starting. JDBC URL: jdbc:postgresql://127.0.0.1:${PORT}/postgres"
  echo "User: ${USER_NAME}  Password: ${PASSWORD}"
elif [[ "${cmd}" == "stop" ]]; then
  podman stop ${NAME} || true
elif [[ "${cmd}" == "rm" ]]; then
  podman rm -f ${NAME} || true
  echo "To wipe data, remove ${DATA_DIR}"
else
  echo "Unknown command: ${cmd}"
  exit 1
fi
