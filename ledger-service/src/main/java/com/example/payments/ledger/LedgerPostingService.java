package com.example.payments.ledger;

import com.example.payments.events.LedgerPosted;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.Topics;
import com.example.payments.outbox.OutboxWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Exactly-once EFFECT on an at-least-once stream: the dedup marker, both ledger legs and the outbox event
 * commit in one PostgreSQL transaction. A redelivered event finds the marker and does nothing.
 */
@Service
public class LedgerPostingService {

    public enum Outcome { POSTED, DUPLICATE }

    private final JdbcTemplate jdbc;
    private final OutboxWriter outbox;

    public LedgerPostingService(JdbcTemplate jdbc, OutboxWriter outbox) {
        this.jdbc = jdbc;
        this.outbox = outbox;
    }

    @Transactional
    public Outcome post(PaymentDecided d) {
        UUID paymentId = UUID.fromString(d.paymentId());

        int claimed = jdbc.update(
                "INSERT INTO processed_payments (payment_id) VALUES (?) ON CONFLICT (payment_id) DO NOTHING",
                paymentId);
        if (claimed == 0) {
            return Outcome.DUPLICATE;
        }

        String debit = "payer:" + d.payerAccountId();
        String credit = "merchant:" + d.merchantId();
        jdbc.update("""
                INSERT INTO ledger_entries (payment_id, leg, account_id, amount_minor, currency)
                VALUES (?, 'DEBIT', ?, ?, ?), (?, 'CREDIT', ?, ?, ?)
                """,
                paymentId, debit, d.amountMinor(), d.currency(),
                paymentId, credit, d.amountMinor(), d.currency());

        outbox.write(Topics.LEDGER_POSTED, d.paymentId(), "LedgerPosted",
                new LedgerPosted(d.paymentId(), debit, credit, d.amountMinor(), d.currency(), System.currentTimeMillis()));
        return Outcome.POSTED;
    }
}
