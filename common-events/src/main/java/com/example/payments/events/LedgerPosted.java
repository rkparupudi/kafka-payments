package com.example.payments.events;

/** Emitted (via the ledger outbox) after both ledger legs are committed. */
public record LedgerPosted(
        String paymentId,
        String debitAccountId,
        String creditAccountId,
        long amountMinor,
        String currency,
        long postedAt) {
}
