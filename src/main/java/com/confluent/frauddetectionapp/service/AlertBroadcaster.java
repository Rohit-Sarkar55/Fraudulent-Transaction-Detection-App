package com.confluent.frauddetectionapp.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Holds all currently-connected dashboard browser tabs (SSE emitters)
 * and pushes each new fraud alert to all of them as it arrives.
 *
 * CopyOnWriteArrayList is intentional here: reads (broadcasting) happen
 * far more often than writes (a tab connecting/disconnecting), and this
 * list is small (a handful of open dashboard tabs at most), so the
 * copy-on-write cost is negligible while iteration stays lock-free.
 */
@Service
public class AlertBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(AlertBroadcaster.class);

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public record Alert(
            String transactionId,
            String cardId,
            String customerId,
            double amount,
            String merchant,
            int riskScore,
            String reasonCode,
            String aiExplanation) {
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        log.info("Dashboard client connected. Active connections: {}", emitters.size());
        return emitter;
    }

    public void broadcast(Alert alert) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("fraud-alert").data(alert));
            } catch (IOException e) {
                emitters.remove(emitter);
            }
        }
    }
}