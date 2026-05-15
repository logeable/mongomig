#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v mongodump >/dev/null 2>&1 || ! command -v mongorestore >/dev/null 2>&1; then
  echo "mongodump and mongorestore must be on PATH (MongoDB Database Tools)" >&2
  exit 1
fi

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-mongomig_e2e}"
docker compose -f docker-compose.e2e.yml up -d --wait mongo minio
sleep 2

docker compose -f docker-compose.e2e.yml exec -T mongo mongosh "mongodb://127.0.0.1:27017/?directConnection=true" --quiet --eval '
  try { rs.status(); } catch (e) {
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongo:27017" }] });
  }
'
for _ in $(seq 1 60); do
  st=$(docker compose -f docker-compose.e2e.yml exec -T mongo mongosh "mongodb://127.0.0.1:27017/?directConnection=true" --quiet --eval 'try { print(rs.status().myState); } catch(e) { print(99); }' | tr -d '\r')
  if [[ "$st" == "1" ]]; then break; fi
  sleep 1
done
if [[ "${st:-}" != "1" ]]; then
  echo "mongo did not become PRIMARY" >&2
  exit 1
fi

docker run --rm --network "${COMPOSE_PROJECT_NAME}_default" minio/mc:RELEASE.2024-05-09T17-04-24Z \
  sh -ec 'mc alias set local http://minio:9000 minio minio12345 && mc mb -p local/mongomig || true'

URI_HOST="mongodb://127.0.0.1:27017/?directConnection=true"
URI_DOCKER='mongodb://mongo:27017/?directConnection=true'
STAGING="$(mktemp -d)"
CHAIN="e2e"

export MONGOMIG_S3_ACCESS_KEY_ID=minio
export MONGOMIG_S3_SECRET_ACCESS_KEY=minio12345

go build -o "$STAGING/mongomig" ./cmd/mongomig

docker compose -f docker-compose.e2e.yml exec -T mongo mongosh "$URI_DOCKER" --quiet --eval '
  db = db.getSiblingDB("mongomig_e2e");
  db.samples.drop();
  db.samples.insertOne({ n: 1, tag: "full" });
'

"$STAGING/mongomig" --mongo-uri "$URI_HOST" --staging-dir "$STAGING/data" --chain "$CHAIN" \
  --s3-endpoint "http://127.0.0.1:9000" --s3-region "us-east-1" --s3-bucket "mongomig" --s3-path-style \
  backup full

docker compose -f docker-compose.e2e.yml exec -T mongo mongosh "$URI_DOCKER" --quiet --eval '
  db = db.getSiblingDB("mongomig_e2e");
  db.samples.insertOne({ n: 2, tag: "incr" });
'

"$STAGING/mongomig" --mongo-uri "$URI_HOST" --staging-dir "$STAGING/data" --chain "$CHAIN" \
  --s3-endpoint "http://127.0.0.1:9000" --s3-region "us-east-1" --s3-bucket "mongomig" --s3-path-style \
  backup incremental

docker compose -f docker-compose.e2e.yml exec -T mongo mongosh "$URI_DOCKER" --quiet --eval '
  db = db.getSiblingDB("mongomig_e2e");
  db.samples.drop();
'

mkdir -p "$STAGING/restore"
"$STAGING/mongomig" --mongo-uri "$URI_HOST" --staging-dir "$STAGING/restore" --chain "$CHAIN" \
  --s3-endpoint "http://127.0.0.1:9000" --s3-region "us-east-1" --s3-bucket "mongomig" --s3-path-style \
  restore --force --drop

COUNT=$(docker compose -f docker-compose.e2e.yml exec -T mongo mongosh "$URI_DOCKER" --quiet --eval '
  db = db.getSiblingDB("mongomig_e2e");
  db.samples.countDocuments({});
' | tr -d '\r')
if [[ "$COUNT" != "2" ]]; then
  echo "expected 2 documents after restore, got '$COUNT'" >&2
  exit 1
fi

echo "e2e ok"

docker compose -f docker-compose.e2e.yml down -v
