package com.example.payments.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.example.payments.api", "com.example.payments.outbox"})
@EnableScheduling
public class PaymentApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentApiApplication.class, args);
    }
}
