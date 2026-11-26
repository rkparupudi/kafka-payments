package com.example.payments.fraud;

import com.example.payments.events.AccountProfile;
import com.example.payments.events.PaymentInitiated;
import com.example.payments.events.PaymentScored;

import java.util.ArrayList;
import java.util.List;

/**
 * Deterministic rule-based scorer standing in for "rules + ML model". It is a pure function of its input,
 * so replays produce identical scores. Thresholds are illustrative.
 * Amounts are in paise: 1 INR = 100 paise.
 */
public class FraudScorer {

    public static final String MODEL_VERSION = "rules-v1";

    static final long INR = 100L;
    static final long HIGH_AMOUNT_NEW_PAYEE = 10_000 * INR;
    static final long LOW_KYC_LIMIT = 5_000 * INR;
    static final long DAILY_LIMIT = 200_000 * INR;
    static final long NEW_ACCOUNT_MS = 7L * 24 * 60 * 60 * 1000;

    public PaymentScored score(PaymentFeatures f) {
        PaymentInitiated p = f.payment();
        AccountProfile profile = f.profile();
        List<String> reasons = new ArrayList<>();
        double score = 0.05;

        if (f.paymentsLast1m() >= 5) {
            score += 0.45;
            reasons.add("VELOCITY_1M");
        }
        if (f.paymentsLast1h() >= 20) {
            score += 0.25;
            reasons.add("VELOCITY_1H");
        }
        if (f.amountLast24hMinor() + p.amountMinor() > DAILY_LIMIT) {
            score += 0.25;
            reasons.add("AMOUNT_24H");
        }
        if (f.newPayee() && p.amountMinor() > HIGH_AMOUNT_NEW_PAYEE) {
            score += 0.20;
            reasons.add("NEW_PAYEE_HIGH_AMOUNT");
        }
        if (profile == null) {
            score += 0.10;
            reasons.add("UNKNOWN_PROFILE");
        } else {
            if (profile.kycLevel() < 2 && p.amountMinor() > LOW_KYC_LIMIT) {
                score += 0.15;
                reasons.add("LOW_KYC");
            }
            if (p.occurredAt() - profile.accountCreatedAt() < NEW_ACCOUNT_MS) {
                score += 0.15;
                reasons.add("NEW_ACCOUNT");
            }
        }
        double rounded = Math.round(Math.min(1.0, score) * 100.0) / 100.0;
        return new PaymentScored(p, rounded, List.copyOf(reasons), MODEL_VERSION, p.occurredAt());
    }
}
