package com.example.payments.api;

import com.example.payments.events.PaymentInitiated;
import com.example.payments.events.Topics;
import com.example.payments.outbox.OutboxWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final OutboxWriter outbox;
    private final TransactionTemplate tx;

    public PaymentService(PaymentRepository payments, OutboxWriter outbox, TransactionTemplate tx) {
        this.payments = payments;
        this.outbox = outbox;
        this.tx = tx;
    }

    /**
     * Idempotent create. The payment row and the outbox event are written in ONE database transaction.
     * A retry with the same (payer, Idempotency-Key) returns the original payment.
     */
    public PaymentView create(String idempotencyKey, CreatePaymentRequest request) {
        var existing = payments.findByIdempotencyKey(request.payerAccountId(), idempotencyKey);
        if (existing.isPresent()) {
            return existing.get();
        }
        UUID paymentId = UUID.randomUUID();
        try {
            tx.executeWithoutResult(status -> {
                payments.insertPending(paymentId, idempotencyKey, request);
                var event = new PaymentInitiated(
                        UUID.randomUUID().toString(), paymentId.toString(), request.payerAccountId(),
                        request.merchantId(), request.amountMinor(), request.currency(), request.channel(),
                        request.deviceId(), System.currentTimeMillis());
                // Kafka key = payerAccountId -> per-account ordering through the pipeline
                outbox.write(Topics.PAYMENTS_INITIATED, request.payerAccountId(), "PaymentInitiated", event);
            });
        } catch (DuplicateKeyException race) {
            // A concurrent request with the same key won; its transaction committed first.
            return payments.findByIdempotencyKey(request.payerAccountId(), idempotencyKey).orElseThrow();
        }
        return payments.findById(paymentId).orElseThrow();
    }
}
