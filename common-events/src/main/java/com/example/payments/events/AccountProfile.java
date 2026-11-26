package com.example.payments.events;

/** Latest profile per account, published to the compacted topic account.profiles (key = accountId). */
public record AccountProfile(
        String accountId,
        int kycLevel,
        long accountCreatedAt) {
}
