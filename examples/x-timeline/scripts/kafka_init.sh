#!/bin/sh
set -eu

BROKERS="${KAFKA_BROKERS:-redpanda:9092}"
TOPIC="${KAFKA_TOPIC:-tweet-events}"

echo "waiting for kafka at ${BROKERS}..."
until rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; do
  sleep 1
done

echo "ensuring topic ${TOPIC} exists..."
until rpk topic describe "${TOPIC}" --brokers "${BROKERS}" >/dev/null 2>&1; do
  rpk topic create "${TOPIC}" --brokers "${BROKERS}" >/dev/null 2>&1 || true
  sleep 1
done

echo "topic ${TOPIC} is ready"
