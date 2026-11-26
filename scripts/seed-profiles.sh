#!/bin/bash
# Publishes account profiles to the compacted topic (key|value). Run after `docker compose up`.
set -euo pipefail
NOW=$(($(date +%s) * 1000))
OLD=$((NOW - 400 * 86400000))
NEW=$((NOW - 2 * 86400000))
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic account.profiles \
  --property parse.key=true --property "key.separator=|" <<RECORDS
acc-100|{"accountId":"acc-100","kycLevel":3,"accountCreatedAt":$OLD}
acc-200|{"accountId":"acc-200","kycLevel":1,"accountCreatedAt":$NEW}
RECORDS
echo "Seeded profiles for acc-100 (established, full KYC) and acc-200 (2 days old, low KYC)"
