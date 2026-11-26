package com.example.payments.api;

import java.time.OffsetDateTime;

public record PaymentView(
        String paymentId,
        String payerAccountId,
        String merchantId,
        long amountMinor,
        String currency,
        String status,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {
}
