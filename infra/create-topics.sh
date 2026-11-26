#!/bin/bash
# Creates topics explicitly. payments.initiated / scored / decided and account.profiles MUST share the
# same partition count and key (payerAccountId / accountId) so Kafka Streams can co-partition the join.
set -euo pipefail
KT=/opt/kafka/bin/kafka-topics.sh
P=6   # 48 in the production sizing; 6 locally

create() { $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 "${@:3}"; }

create payments.initiated     $P --config retention.ms=604800000
create payments.scored        $P --config retention.ms=604800000
create payments.decided       $P --config retention.ms=604800000
create account.profiles       $P --config cleanup.policy=compact
create ledger.posted          3  --config retention.ms=2592000000
create payments.decided.dlq   1  --config retention.ms=1209600000

$KT --bootstrap-server "$BOOTSTRAP" --list
