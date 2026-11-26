package com.example.payments.ledger;

import com.example.payments.events.Decision;
import com.example.payments.events.Json;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Offsets are committed by the listener container only AFTER this method returns, i.e. after the DB commit
 * (enable.auto.commit=false). A crash in between causes redelivery, which LedgerPostingService absorbs.
 */
@Component
public class DecisionConsumer {

    private static final Logger log = LoggerFactory.getLogger(DecisionConsumer.class);

    private final LedgerPostingService ledger;

    public DecisionConsumer(LedgerPostingService ledger) {
        this.ledger = ledger;
    }

    @KafkaListener(topics = Topics.PAYMENTS_DECIDED, groupId = "ledger-service", concurrency = "3")
    public void onDecision(String json) {
        PaymentDecided decided = Json.read(json, PaymentDecided.class); // bad JSON -> DLQ (not retryable)
        if (decided.decision() != Decision.APPROVED) {
            return;
        }
        var outcome = ledger.post(decided);
        log.info("ledger {} for payment {} ({} {} paise)", outcome, decided.paymentId(),
                decided.currency(), decided.amountMinor());
    }
}
