package com.example.payments.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.TimeZone;

@SpringBootApplication(scanBasePackages = {"com.example.payments.api", "com.example.payments.outbox"})
@EnableScheduling
public class PaymentApiApplication {
    public static void main(String[] args) {
        // Run in UTC: timestamps are stored as TIMESTAMPTZ, and the JDBC driver sends the JVM zone to Postgres.
        // Some OS zone ids (e.g. the legacy alias "Asia/Calcutta") are rejected by Postgres at connect time.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(PaymentApiApplication.class, args);
    }
}
