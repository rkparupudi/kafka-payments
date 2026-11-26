package com.example.payments.outbox;

import com.example.payments.events.Json;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Writes an outbox row. MANDATORY propagation forces callers to invoke it inside their own business
 * transaction, so the business row and the event commit or roll back together (no dual write).
 */
@Component
public class OutboxWriter {

    private final JdbcTemplate jdbc;

    public OutboxWriter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void write(String topic, String key, String type, Object payload) {
        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb))
                """, UUID.randomUUID(), topic, key, type, Json.write(payload));
    }
}
