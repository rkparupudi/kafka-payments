package com.example.payments.api;

import com.example.payments.events.Json;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/** Updates payment status from decisions. Idempotent: only PENDING rows change. */
@Component
public class PaymentStatusConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusConsumer.class);

    private final PaymentRepository payments;

    public PaymentStatusConsumer(PaymentRepository payments) {
        this.payments = payments;
    }

    @KafkaListener(topics = Topics.PAYMENTS_DECIDED, groupId = "payment-api-status")
    public void onDecision(String json) {
        PaymentDecided decided = Json.read(json, PaymentDecided.class);
        int updated = payments.applyDecision(UUID.fromString(decided.paymentId()), decided.decision().name());
        log.info("payment {} -> {} (score {}, reasons {}){}", decided.paymentId(), decided.decision(),
                decided.score(), decided.reasons(), updated == 0 ? " [duplicate ignored]" : "");
    }
}
