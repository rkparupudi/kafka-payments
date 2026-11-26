package com.example.payments.events;

import java.util.List;

/** Output of fraud scoring. The model version is stamped so replays and audits are reproducible. */
public record PaymentScored(
        PaymentInitiated payment,
        double score,
        List<String> reasons,
        String modelVersion,
        long scoredAt) {
}
