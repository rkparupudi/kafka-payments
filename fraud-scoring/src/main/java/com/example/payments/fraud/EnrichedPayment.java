package com.example.payments.fraud;

import com.example.payments.events.AccountProfile;
import com.example.payments.events.PaymentInitiated;

/** Payment joined with the payer's profile (profile may be null if not yet known). */
public record EnrichedPayment(PaymentInitiated payment, AccountProfile profile) {
}
