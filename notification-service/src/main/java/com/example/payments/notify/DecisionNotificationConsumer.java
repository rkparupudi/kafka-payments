package com.example.payments.notify;

import com.example.payments.events.Json;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * At-least-once delivery + best-effort dedup. SETNX claims the event id for 24h; if sending fails the claim is
 * released so the redelivered record can try again. A crash between send and offset commit can still produce
 * a duplicate SMS — acceptable for notifications, never for money (see ledger-service).
 */
@Component
public class DecisionNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(DecisionNotificationConsumer.class);
    private static final Duration DEDUP_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redis;
    private final NotificationSender sender;

    public DecisionNotificationConsumer(StringRedisTemplate redis, NotificationSender sender) {
        this.redis = redis;
        this.sender = sender;
    }

    @KafkaListener(topics = Topics.PAYMENTS_DECIDED, groupId = "notification-service")
    public void onDecision(String json) {
        PaymentDecided d = Json.read(json, PaymentDecided.class);
        String key = "dedup:notif:" + d.eventId();
        Boolean claimed = redis.opsForValue().setIfAbsent(key, "1", DEDUP_TTL);
        if (!Boolean.TRUE.equals(claimed)) {
            log.debug("duplicate {}", d.eventId());
            return;
        }
        try {
            sender.send(d);
        } catch (RuntimeException e) {
            redis.delete(key);
            throw e; // container retries / error handler
        }
    }
}
