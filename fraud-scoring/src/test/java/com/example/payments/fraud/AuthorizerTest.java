package com.example.payments.fraud;

import com.example.payments.events.Decision;
import com.example.payments.events.PaymentInitiated;
import com.example.payments.events.PaymentScored;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuthorizerTest {

    @ParameterizedTest
    @CsvSource({"0.05,APPROVED", "0.59,APPROVED", "0.60,STEP_UP", "0.89,STEP_UP", "0.90,DECLINED", "1.0,DECLINED"})
    void mapsScoreToDecision(double score, Decision expected) {
        var p = new PaymentInitiated("e", "p-1", "acc", "m", 100, "INR", "UPI", null, 0L);
        var decided = new Authorizer().decide(new PaymentScored(p, score, List.of(), "rules-v1", 0L));
        assertThat(decided.decision()).isEqualTo(expected);
        assertThat(decided.eventId()).isEqualTo("p-1:decided"); // deterministic for replay dedup
    }
}
