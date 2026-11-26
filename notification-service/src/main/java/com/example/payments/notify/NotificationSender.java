package com.example.payments.notify;

import com.example.payments.events.PaymentDecided;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Stand-in for an SMS / push provider client. */
@Component
public class NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(NotificationSender.class);

    public void send(PaymentDecided d) {
        String text = switch (d.decision()) {
            case APPROVED -> "Paid Rs %.2f to %s".formatted(d.amountMinor() / 100.0, d.merchantId());
            case DECLINED -> "Payment of Rs %.2f to %s was declined".formatted(d.amountMinor() / 100.0, d.merchantId());
            case STEP_UP -> "Enter the OTP to complete your payment to %s".formatted(d.merchantId());
        };
        log.info("NOTIFY {} -> {}", d.payerAccountId(), text);
    }
}
