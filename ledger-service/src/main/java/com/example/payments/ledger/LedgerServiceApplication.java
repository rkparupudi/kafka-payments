package com.example.payments.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.example.payments.ledger", "com.example.payments.outbox"})
@EnableScheduling
public class LedgerServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
    }
}
