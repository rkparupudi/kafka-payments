package com.example.payments.fraud;

import com.example.payments.events.Decision;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.PaymentInitiated;
import com.example.payments.events.PaymentScored;

/** Maps a score to a decision. Pure function -> deterministic on replay. */
public class Authorizer {

    static final double DECLINE_AT = 0.90;
    static final double STEP_UP_AT = 0.60;

    public PaymentDecided decide(PaymentScored scored) {
        PaymentInitiated p = scored.payment();
        Decision decision = scored.score() >= DECLINE_AT ? Decision.DECLINED
                : scored.score() >= STEP_UP_AT ? Decision.STEP_UP
                : Decision.APPROVED;
        return new PaymentDecided(
                // Deterministic event id: a replay yields the same id, so consumers can dedup on it.
                p.paymentId() + ":decided",
                p.paymentId(), p.payerAccountId(), p.merchantId(), p.amountMinor(), p.currency(),
                decision, scored.score(), scored.reasons(), scored.scoredAt());
    }
}
