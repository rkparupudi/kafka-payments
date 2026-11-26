package com.example.payments.events;

import java.util.List;

public record PaymentDecided(
        String eventId,
        String paymentId,
        String payerAccountId,
        String merchantId,
        long amountMinor,
        String currency,
        Decision decision,
        double score,
        List<String> reasons,
        long decidedAt) {
}
