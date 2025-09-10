#!/usr/bin/env bash
# Simple helper to run a local Oracle Free (XE) for testing with Podman.
# Uses gvenzl/oracle-free image which is easy for local development.
# Usage: ./scripts/podman-oracle.sh start|stop|rm
# Data is persisted under ./tmp/oracle
# Default PDB: FREEPDB1

set -euo pipefail

NAME="globs-oracle"
DATA_DIR="$(pwd)/tmp/oracle"
IMAGE="docker.io/gvenzl/oracle-free:23"
# SYSTEM password
PASSWORD="oracle"
PORT_SQL="1521"
PORT_EM="5500"

mkdir -p "${DATA_DIR}"

cmd=${1:-start}

if [[ "${cmd}" == "start" ]]; then
  echo "Starting ${NAME} on port ${PORT_SQL}..."
  podman run -d --name ${NAME} \
    -v "${DATA_DIR}:/opt/oracle/oradata:Z" \
    -e ORACLE_PASSWORD=${PASSWORD} \
    -p ${PORT_SQL}:1521 \
    -p ${PORT_EM}:5500 \
    ${IMAGE}
  echo "Oracle is starting. It can take a few minutes on first run."
  echo "JDBC URL: jdbc:oracle:thin:@//127.0.0.1:${PORT_SQL}/FREEPDB1"
  echo "User: system  Password: ${PASSWORD}"
elif [[ "${cmd}" == "stop" ]]; then
  podman stop ${NAME} || true
elif [[ "${cmd}" == "rm" ]]; then
  podman rm -f ${NAME} || true
  echo "To wipe data, remove ${DATA_DIR}"
else
  echo "Unknown command: ${cmd}"
  exit 1
fi
