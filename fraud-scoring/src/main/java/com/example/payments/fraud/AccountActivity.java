package com.example.payments.fraud;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Per-account state kept in a RocksDB store (backed by a changelog topic).
 * Bounded: only the last 24h of payments (max MAX_EVENTS) and up to MAX_PAYEES known payees.
 */
public record AccountActivity(List<Entry> recent, List<String> knownPayees) {

    public record Entry(long at, long amountMinor) {
    }

    static final long DAY_MS = 24L * 60 * 60 * 1000;
    static final int MAX_EVENTS = 500;
    static final int MAX_PAYEES = 200;

    public static AccountActivity empty() {
        return new AccountActivity(List.of(), List.of());
    }

    public int countSince(long fromInclusive) {
        return (int) recent.stream().filter(e -> e.at() >= fromInclusive).count();
    }

    public long amountSince(long fromInclusive) {
        return recent.stream().filter(e -> e.at() >= fromInclusive).mapToLong(Entry::amountMinor).sum();
    }

    public boolean knowsPayee(String payeeId) {
        return knownPayees.contains(payeeId);
    }

    /** Returns a new state including this payment, pruned relative to the newest event time seen. */
    public AccountActivity record(long at, long amountMinor, String payeeId) {
        long newest = Math.max(at, recent.stream().mapToLong(Entry::at).max().orElse(at));
        List<Entry> next = new ArrayList<>(recent.size() + 1);
        for (Entry e : recent) {
            if (e.at() >= newest - DAY_MS) {
                next.add(e);
            }
        }
        next.add(new Entry(at, amountMinor));
        if (next.size() > MAX_EVENTS) {
            next = new ArrayList<>(next.subList(next.size() - MAX_EVENTS, next.size()));
        }

        LinkedHashSet<String> payees = new LinkedHashSet<>(knownPayees);
        payees.remove(payeeId);
        payees.add(payeeId); // most recent last
        List<String> payeeList = new ArrayList<>(payees);
        if (payeeList.size() > MAX_PAYEES) {
            payeeList = new ArrayList<>(payeeList.subList(payeeList.size() - MAX_PAYEES, payeeList.size()));
        }
        return new AccountActivity(List.copyOf(next), List.copyOf(payeeList));
    }
}
