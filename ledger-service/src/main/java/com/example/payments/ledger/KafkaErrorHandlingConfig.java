package com.example.payments.ledger;

import com.example.payments.events.Topics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Bounded retries with exponential backoff, then publish to "<topic>.dlq" with the original key and
 * exception headers. A poison message no longer blocks its partition.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + Topics.DLQ_SUFFIX, -1)); // -1: let Kafka pick
        var backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxElapsedTime(10_000L);
        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class); // malformed payloads go straight to DLQ
        return handler;
    }
}
