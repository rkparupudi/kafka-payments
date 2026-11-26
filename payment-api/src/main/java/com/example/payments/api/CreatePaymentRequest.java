package com.example.payments.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

public record CreatePaymentRequest(
        @NotBlank String payerAccountId,
        @NotBlank String merchantId,
        @Positive long amountMinor,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotBlank @Pattern(regexp = "UPI|CARD") String channel,
        String deviceId) {
}
