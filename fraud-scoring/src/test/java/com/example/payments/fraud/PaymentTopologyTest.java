package com.example.payments.fraud;

import com.example.payments.events.AccountProfile;
import com.example.payments.events.Decision;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.PaymentInitiated;
import com.example.payments.events.PaymentScored;
import com.example.payments.events.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentTopologyTest {

    private static final long T0 = Instant.parse("2026-11-26T06:00:00Z").toEpochMilli();
    private static final long DAY = 24L * 60 * 60 * 1000;

    private TopologyTestDriver driver;
    private TestInputTopic<String, AccountProfile> profiles;
    private TestInputTopic<String, PaymentInitiated> initiated;
    private TestOutputTopic<String, PaymentScored> scored;
    private TestOutputTopic<String, PaymentDecided> decided;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        PaymentTopology.build(builder, new FraudScorer(), new Authorizer());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-scoring-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/streams-test-" + UUID.randomUUID());
        driver = new TopologyTestDriver(builder.build(), props);

        profiles = driver.createInputTopic(Topics.ACCOUNT_PROFILES,
                Serdes.String().serializer(), PaymentTopology.PROFILE.serializer());
        initiated = driver.createInputTopic(Topics.PAYMENTS_INITIATED,
                Serdes.String().serializer(), PaymentTopology.INITIATED.serializer());
        scored = driver.createOutputTopic(Topics.PAYMENTS_SCORED,
                Serdes.String().deserializer(), PaymentTopology.SCORED.deserializer());
        decided = driver.createOutputTopic(Topics.PAYMENTS_DECIDED,
                Serdes.String().deserializer(), PaymentTopology.DECIDED.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static PaymentInitiated payment(String payer, String merchant, long amountInr, long at) {
        return new PaymentInitiated(UUID.randomUUID().toString(), UUID.randomUUID().toString(), payer, merchant,
                amountInr * 100, "INR", "UPI", "device-1", at);
    }

    @Test
    void ordinaryPaymentFromKnownAccountIsApproved() {
        profiles.pipeInput("acc-1", new AccountProfile("acc-1", 3, T0 - 400 * DAY), Instant.ofEpochMilli(T0 - 1));
        initiated.pipeInput("acc-1", payment("acc-1", "m-1", 250, T0), Instant.ofEpochMilli(T0));

        PaymentScored s = scored.readValue();
        assertThat(s.score()).isEqualTo(0.05);
        assertThat(s.reasons()).isEmpty();
        assertThat(decided.readValue().decision()).isEqualTo(Decision.APPROVED);
    }

    @Test
    void burstOfPaymentsTriggersVelocityRule() {
        profiles.pipeInput("acc-2", new AccountProfile("acc-2", 3, T0 - 400 * DAY), Instant.ofEpochMilli(T0 - 1));
        for (int i = 0; i < 6; i++) {
            long at = T0 + i * 5_000L; // six payments within 30 seconds
            initiated.pipeInput("acc-2", payment("acc-2", "m-1", 100, at), Instant.ofEpochMilli(at));
        }
        List<PaymentScored> scores = scored.readValuesToList();
        assertThat(scores).hasSize(6);
        assertThat(scores.get(4).reasons()).doesNotContain("VELOCITY_1M"); // 4 earlier payments
        assertThat(scores.get(5).reasons()).contains("VELOCITY_1M");       // 5 earlier payments
        assertThat(scores.get(5).score()).isEqualTo(0.50);
    }

    @Test
    void riskyPaymentFromNewLowKycAccountIsDeclined() {
        profiles.pipeInput("acc-3", new AccountProfile("acc-3", 1, T0 - 2 * DAY), Instant.ofEpochMilli(T0 - 1));
        // Burst of five small payments, then a large one to a new payee that breaches the daily limit.
        for (int i = 0; i < 5; i++) {
            long at = T0 + i * 1_000L;
            initiated.pipeInput("acc-3", payment("acc-3", "m-1", 10, at), Instant.ofEpochMilli(at));
        }
        long at = T0 + 10_000L;
        initiated.pipeInput("acc-3", payment("acc-3", "m-new", 250_000, at), Instant.ofEpochMilli(at));

        List<PaymentDecided> decisions = decided.readValuesToList();
        PaymentDecided last = decisions.get(decisions.size() - 1);
        // 0.05 + VELOCITY_1M 0.45 + AMOUNT_24H 0.25 + NEW_PAYEE 0.20 + LOW_KYC 0.15 + NEW_ACCOUNT 0.15 -> capped at 1.0
        assertThat(last.score()).isEqualTo(1.0);
        assertThat(last.decision()).isEqualTo(Decision.DECLINED);
        assertThat(last.reasons()).contains("VELOCITY_1M", "AMOUNT_24H", "NEW_PAYEE_HIGH_AMOUNT", "LOW_KYC", "NEW_ACCOUNT");
    }

    @Test
    void unknownProfileIsScoredNotDropped() {
        initiated.pipeInput("acc-4", payment("acc-4", "m-1", 100, T0), Instant.ofEpochMilli(T0));
        PaymentScored s = scored.readValue();
        assertThat(s.reasons()).containsExactly("UNKNOWN_PROFILE");
        assertThat(s.score()).isEqualTo(0.15);
    }
}
