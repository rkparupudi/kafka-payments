package com.example.payments.events;

/** Topic names. initiated / scored / decided / account.profiles must share key and partition count (co-partitioning). */
public final class Topics {
    public static final String PAYMENTS_INITIATED = "payments.initiated";
    public static final String PAYMENTS_SCORED = "payments.scored";
    public static final String PAYMENTS_DECIDED = "payments.decided";
    public static final String LEDGER_POSTED = "ledger.posted";
    public static final String ACCOUNT_PROFILES = "account.profiles";
    public static final String DLQ_SUFFIX = ".dlq";

    private Topics() {
    }
}
