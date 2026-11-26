package com.example.payments.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public class PaymentRepository {

    private static final RowMapper<PaymentView> MAPPER = (rs, i) -> new PaymentView(
            rs.getObject("payment_id", UUID.class).toString(),
            rs.getString("payer_account_id"),
            rs.getString("merchant_id"),
            rs.getLong("amount_minor"),
            rs.getString("currency"),
            rs.getString("status"),
            rs.getObject("created_at", OffsetDateTime.class),
            rs.getObject("updated_at", OffsetDateTime.class));

    private final JdbcTemplate jdbc;

    public PaymentRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertPending(UUID paymentId, String idempotencyKey, CreatePaymentRequest r) {
        jdbc.update("""
                INSERT INTO payments (payment_id, idempotency_key, payer_account_id, merchant_id,
                                      amount_minor, currency, channel, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
                """, paymentId, idempotencyKey, r.payerAccountId(), r.merchantId(),
                r.amountMinor(), r.currency(), r.channel());
    }

    public Optional<PaymentView> findById(UUID paymentId) {
        return jdbc.query("SELECT * FROM payments WHERE payment_id = ?", MAPPER, paymentId).stream().findFirst();
    }

    public Optional<PaymentView> findByIdempotencyKey(String payerAccountId, String idempotencyKey) {
        return jdbc.query("SELECT * FROM payments WHERE payer_account_id = ? AND idempotency_key = ?",
                MAPPER, payerAccountId, idempotencyKey).stream().findFirst();
    }

    /** Only moves PENDING payments, so replayed or duplicate decision events are no-ops. */
    public int applyDecision(UUID paymentId, String status) {
        return jdbc.update("""
                UPDATE payments SET status = ?, updated_at = now()
                WHERE payment_id = ? AND status = 'PENDING'
                """, status, paymentId);
    }
}
