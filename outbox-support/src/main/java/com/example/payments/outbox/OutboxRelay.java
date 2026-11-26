package com.example.payments.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Polling outbox relay for local development. In production, replace with Debezium's Postgres connector
 * + Outbox Event Router (see infra/debezium-outbox-connector.json): it reads the WAL instead of polling.
 *
 * Guarantees: at-least-once. If the process dies after Kafka acks but before the DELETE commits,
 * rows are re-sent; downstream consumers deduplicate on paymentId / eventId.
 *
 * Ordering: run ONE relay instance per database (or use Debezium). Several instances with
 * SKIP LOCKED could publish two events for the same key out of order.
 */
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private record Row(UUID id, String topic, String key, String payload) {
    }

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
                       @Value("${outbox.relay.batch-size:200}") int batchSize) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:100}")
    @Transactional
    public void publishBatch() throws Exception {
        List<Row> rows = jdbc.query("""
                SELECT id, aggregate_type, aggregate_id, payload::text AS payload
                FROM outbox
                ORDER BY created_at, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """, (rs, i) -> new Row(rs.getObject("id", UUID.class), rs.getString("aggregate_type"),
                rs.getString("aggregate_id"), rs.getString("payload")), batchSize);
        if (rows.isEmpty()) {
            return;
        }

        // Send the whole batch, then wait for every ack (acks=all) before deleting.
        List<CompletableFuture<SendResult<String, String>>> acks = rows.stream()
                .map(r -> kafka.send(r.topic(), r.key(), r.payload()))
                .toList();
        for (CompletableFuture<SendResult<String, String>> ack : acks) {
            ack.get(10, TimeUnit.SECONDS); // throws -> transaction rolls back -> rows retried next poll
        }

        jdbc.batchUpdate("DELETE FROM outbox WHERE id = ?",
                rows.stream().map(r -> new Object[]{r.id()}).toList());
        log.debug("Relayed {} outbox events", rows.size());
    }
}
