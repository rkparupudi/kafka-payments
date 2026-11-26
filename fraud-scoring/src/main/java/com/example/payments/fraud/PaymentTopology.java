package com.example.payments.fraud;

import com.example.payments.events.AccountProfile;
import com.example.payments.events.Json;
import com.example.payments.events.PaymentDecided;
import com.example.payments.events.PaymentInitiated;
import com.example.payments.events.PaymentScored;
import com.example.payments.events.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * Sub-topology 1 (fraud scoring):
 *   payments.initiated --leftJoin(account.profiles KTable)--> velocity features (RocksDB) --> score --> payments.scored
 * Sub-topology 2 (authorization):
 *   payments.scored --> decide --> payments.decided
 *
 * Both run with processing.guarantee=exactly_once_v2: consume, state update, produce and offset commit are
 * one Kafka transaction. In a larger system authorization would be its own deployable; it shares an app here
 * for brevity.
 */
public final class PaymentTopology {

    static final Serde<String> KEY = Serdes.String();
    static final Serde<PaymentInitiated> INITIATED = Json.serde(PaymentInitiated.class);
    static final Serde<AccountProfile> PROFILE = Json.serde(AccountProfile.class);
    static final Serde<EnrichedPayment> ENRICHED = Json.serde(EnrichedPayment.class);
    static final Serde<AccountActivity> ACTIVITY = Json.serde(AccountActivity.class);
    static final Serde<PaymentScored> SCORED = Json.serde(PaymentScored.class);
    static final Serde<PaymentDecided> DECIDED = Json.serde(PaymentDecided.class);

    private PaymentTopology() {
    }

    public static KStream<String, PaymentInitiated> build(StreamsBuilder builder, FraudScorer scorer, Authorizer authorizer) {
        // Compacted topic -> KTable. Must be co-partitioned with payments.initiated (same key, same partition count).
        KTable<String, AccountProfile> profiles = builder.table(
                Topics.ACCOUNT_PROFILES,
                Consumed.with(KEY, PROFILE),
                Materialized.as("account-profiles-store"));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(VelocityProcessor.STORE), KEY, ACTIVITY));

        KStream<String, PaymentInitiated> initiated =
                builder.stream(Topics.PAYMENTS_INITIATED, Consumed.with(KEY, INITIATED));

        initiated
                .leftJoin(profiles, EnrichedPayment::new, Joined.with(KEY, INITIATED, PROFILE))
                .processValues(VelocityProcessor::new, VelocityProcessor.STORE)
                .mapValues(scorer::score)
                .to(Topics.PAYMENTS_SCORED, Produced.with(KEY, SCORED));

        builder.stream(Topics.PAYMENTS_SCORED, Consumed.with(KEY, SCORED))
                .mapValues(authorizer::decide)
                .to(Topics.PAYMENTS_DECIDED, Produced.with(KEY, DECIDED));

        return initiated;
    }
}
