package com.example.payments.fraud;

import com.example.payments.events.AccountProfile;
import com.example.payments.events.PaymentInitiated;

/** Real-time features computed from the payer's recent activity, before counting the current payment. */
public record PaymentFeatures(
        PaymentInitiated payment,
        AccountProfile profile,
        int paymentsLast1m,
        int paymentsLast1h,
        long amountLast24hMinor,
        boolean newPayee) {
}
