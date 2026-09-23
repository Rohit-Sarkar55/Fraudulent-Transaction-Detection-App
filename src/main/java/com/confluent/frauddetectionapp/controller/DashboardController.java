package com.confluent.frauddetectionapp.controller;


import com.confluent.frauddetectionapp.service.AlertBroadcaster;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * The dashboard's live feed. dashboard.html opens an EventSource
 * against this endpoint and receives a 'fraud-alert' event for every
 * new row FraudAlertConsumer reads off fraud_alerts_explained.
 */
@RestController
public class DashboardController {

    private final AlertBroadcaster broadcaster;

    public DashboardController(AlertBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping("/api/dashboard/stream")
    public SseEmitter stream() {
        return broadcaster.subscribe();
    }
}
