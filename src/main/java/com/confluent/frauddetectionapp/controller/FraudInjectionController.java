package com.confluent.frauddetectionapp.controller;

import com.confluent.frauddetectionapp.producer.TransactionProducer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Manual triggers for the live demo — call these right before your demo
 * slot (or during it) to guarantee a fraud alert fires on cue, instead of
 * waiting for the random injection rate to happen to produce one.
 *
 * POST /api/demo/inject/velocity
 * POST /api/demo/inject/geo
 *
 * The response includes the transaction_id(s) just created, so you can
 * paste one straight into Confluent Cloud's topic message search box
 * to trace it through the pipeline, without digging through app logs.
 */
@RestController
public class FraudInjectionController {

    private final TransactionProducer producer;

    public FraudInjectionController(TransactionProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/api/demo/inject/{pattern}")
    public Map<String, Object> inject(@PathVariable String pattern) {
        List<String> transactionIds = switch (pattern) {
            case "velocity" -> producer.injectVelocityBurst();
            case "geo" -> producer.injectGeoMismatch();
            default -> null;
        };

        if (transactionIds == null) {
            return Map.of("status", "unknown pattern, use 'velocity' or 'geo'");
        }
        return Map.of(
                "status", "injected",
                "pattern", pattern,
                "transactionIds", transactionIds
        );
    }
}