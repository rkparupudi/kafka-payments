package com.example.payments.api;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService service;
    private final PaymentRepository payments;

    public PaymentController(PaymentService service, PaymentRepository payments) {
        this.service = service;
        this.payments = payments;
    }

    /** Async: returns 202 with status PENDING; the decision arrives via payments.decided. */
    @PostMapping
    public ResponseEntity<PaymentView> create(@RequestHeader("Idempotency-Key") String idempotencyKey,
                                              @Valid @RequestBody CreatePaymentRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(service.create(idempotencyKey, request));
    }

    @GetMapping("/{paymentId}")
    public ResponseEntity<PaymentView> get(@PathVariable UUID paymentId) {
        return ResponseEntity.of(payments.findById(paymentId));
    }
}
