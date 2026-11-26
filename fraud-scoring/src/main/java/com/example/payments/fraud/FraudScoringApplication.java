package com.example.payments.fraud;

import com.example.payments.events.PaymentInitiated;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class FraudScoringApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudScoringApplication.class, args);
    }

    @Bean
    FraudScorer fraudScorer() {
        return new FraudScorer();
    }

    @Bean
    Authorizer authorizer() {
        return new Authorizer();
    }

    /** Spring's StreamsBuilderFactoryBean supplies the builder and starts/stops the KafkaStreams instance. */
    @Bean
    KStream<String, PaymentInitiated> paymentTopology(StreamsBuilder builder, FraudScorer scorer, Authorizer authorizer) {
        return PaymentTopology.build(builder, scorer, authorizer);
    }
}
