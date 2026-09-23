package com.confluent.frauddetectionapp.controller;

import com.confluent.frauddetectionapp.producer.TransactionProducer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Manual triggers for the live demo — call these right before your demo
 * slot (or during it) to guarantee a fraud alert fires on cue, instead of
 * waiting for the random injection rate to happen to produce one.
 *
 * POST /api/demo/inject/velocity
 * POST /api/demo/inject/geo
 * POST /api/demo/inject/amount
 */
@RestController
public class FraudInjectionController {

    private final TransactionProducer producer;

    public FraudInjectionController(TransactionProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/api/demo/inject/{pattern}")
    public Map<String, String> inject(@PathVariable String pattern) {
        switch (pattern) {
            case "velocity" -> producer.injectVelocityBurst();
            case "geo" -> producer.injectGeoMismatch();
            default -> {
                return Map.of("status", "unknown pattern, use 'velocity' or 'geo'");
            }
        }
        return Map.of("status", "injected", "pattern", pattern);
    }
}