#!/bin/sh
set -eu

BROKERS="${KAFKA_BROKERS:-redpanda:9092}"
TOPIC="${KAFKA_TOPIC:-tweet-events}"
INTERVAL="${INGEST_INTERVAL_SECONDS:-0.5}"

echo "waiting for kafka at ${BROKERS}..."
until rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; do
  sleep 1
done

echo "ensuring topic ${TOPIC} exists..."
until rpk topic describe "${TOPIC}" --brokers "${BROKERS}" >/dev/null 2>&1; do
  rpk topic create "${TOPIC}" --brokers "${BROKERS}" >/dev/null 2>&1 || true
  sleep 1
done

echo "starting fake x-timeline ingestion into topic ${TOPIC}"

i=1
while true; do
  author_idx=$(( (i % 5) + 1 ))
  author_id="user-${author_idx}"
  post_id="post-${i}"
  created_at_ms="$(($(date +%s) * 1000))"

  create_payload="$(cat <<EOF
{"event":"create","id":"${post_id}","author_id":"${author_id}","body":"fake post ${i}","created_at":"${created_at_ms}","conversation_id":"conv-${author_idx}"}
EOF
)"

  until printf '%s\n' "${create_payload}" | rpk topic produce "${TOPIC}" --brokers "${BROKERS}" >/dev/null 2>&1; do
    sleep 1
  done

  if [ $((i % 15)) -eq 0 ]; then
    delete_idx=$((i - 10))
    if [ "${delete_idx}" -gt 0 ]; then
      delete_author_idx=$(( (delete_idx % 5) + 1 ))
      delete_payload="$(cat <<EOF
{"event":"delete","id":"post-${delete_idx}","author_id":"user-${delete_author_idx}","created_at":"${created_at_ms}"}
EOF
)"
      until printf '%s\n' "${delete_payload}" | rpk topic produce "${TOPIC}" --brokers "${BROKERS}" >/dev/null 2>&1; do
        sleep 1
      done
    fi
  fi

  i=$((i + 1))
  sleep "${INTERVAL}"
done
