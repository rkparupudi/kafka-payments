#!/bin/bash
# Drives the happy path and a fraud case through the running services.
set -euo pipefail
API=${API:-http://localhost:8080}

pay() { # payer merchant amountMinor idemKey
  curl -s -X POST "$API/payments" -H "Content-Type: application/json" -H "Idempotency-Key: $4" \
    -d "{\"payerAccountId\":\"$1\",\"merchantId\":\"$2\",\"amountMinor\":$3,\"currency\":\"INR\",\"channel\":\"UPI\",\"deviceId\":\"dev-1\"}"
  echo
}

echo "== 1. Normal payment (Rs 250) from an established account"
R=$(pay acc-100 merchant-chai 25000 demo-1); echo "$R"
ID=$(echo "$R" | sed -E 's/.*"paymentId":"([^"]+)".*/\1/')

echo "== 2. Retry with the SAME Idempotency-Key -> same paymentId, no second event"
pay acc-100 merchant-chai 25000 demo-1

echo "== 3. Risky: new low-KYC account, burst + large payment to a new payee"
for i in 1 2 3 4 5; do pay acc-200 merchant-chai 1000 "burst-$i-$RANDOM" > /dev/null; done
R2=$(pay acc-200 merchant-electronics 25000000 "big-$RANDOM"); echo "$R2"
ID2=$(echo "$R2" | sed -E 's/.*"paymentId":"([^"]+)".*/\1/')

sleep 3
echo "== Final status"
curl -s "$API/payments/$ID";  echo
curl -s "$API/payments/$ID2"; echo
echo "Check the ledger:  docker exec -it postgres psql -U payments -d ledger -c 'SELECT payment_id, leg, account_id, amount_minor FROM ledger_entries;'"
