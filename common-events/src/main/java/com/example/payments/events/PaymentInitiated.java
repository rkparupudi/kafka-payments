package com.example.payments.events;

/**
 * Emitted (via the payments outbox) when a payment is accepted.
 * Amounts are minor units (paise) — never floating point. Timestamps are epoch millis (event time).
 */
public record PaymentInitiated(
        String eventId,
        String paymentId,
        String payerAccountId,
        String merchantId,
        long amountMinor,
        String currency,
        String channel,
        String deviceId,
        long occurredAt) {
}
