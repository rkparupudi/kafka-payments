package com.example.payments.fraud;

import com.example.payments.events.PaymentInitiated;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Computes velocity features per payer using EVENT time (occurredAt), so out-of-order events are counted
 * against the right windows. Key is unchanged (payerAccountId), so no repartition topic is created.
 * Under exactly_once_v2 the store's changelog write commits atomically with the output and input offsets.
 */
public class VelocityProcessor implements FixedKeyProcessor<String, EnrichedPayment, PaymentFeatures> {

    public static final String STORE = "account-activity";

    private FixedKeyProcessorContext<String, PaymentFeatures> context;
    private KeyValueStore<String, AccountActivity> store;

    @Override
    public void init(FixedKeyProcessorContext<String, PaymentFeatures> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, EnrichedPayment> record) {
        PaymentInitiated p = record.value().payment();
        AccountActivity activity = store.get(record.key());
        if (activity == null) {
            activity = AccountActivity.empty();
        }
        long t = p.occurredAt();
        PaymentFeatures features = new PaymentFeatures(
                p,
                record.value().profile(),
                activity.countSince(t - 60_000L),
                activity.countSince(t - 3_600_000L),
                activity.amountSince(t - AccountActivity.DAY_MS),
                !activity.knowsPayee(p.merchantId()));

        store.put(record.key(), activity.record(t, p.amountMinor(), p.merchantId()));
        context.forward(record.withValue(features));
    }
}
