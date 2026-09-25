package com.confluent.frauddetectionapp.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AlertBroadcasterTest {

    private AlertBroadcaster broadcaster;

    @BeforeEach
    void setUp() {
        broadcaster = new AlertBroadcaster();
    }

    @Test
    void subscribe_returnsNonNullEmitter() {
        assertNotNull(broadcaster.subscribe());
    }

    @Test
    void subscribe_addsEmitterToList() throws Exception {
        broadcaster.subscribe();
        assertThat(getEmitters()).hasSize(1);
    }

    @Test
    void subscribe_multipleClients_allTracked() throws Exception {
        broadcaster.subscribe();
        broadcaster.subscribe();
        broadcaster.subscribe();
        assertThat(getEmitters()).hasSize(3);
    }

    @Test
    void broadcast_doesNotThrowWithNoClients() {
        AlertBroadcaster.Alert alert = alert();
        assertDoesNotThrow(() -> broadcaster.broadcast(alert));
    }

    @Test
    void broadcastTransaction_doesNotThrowWithNoClients() {
        AlertBroadcaster.ScoredTransaction txn = scoredTransaction();
        assertDoesNotThrow(() -> broadcaster.broadcastTransaction(txn));
    }

    @Test
    void broadcast_removesEmitterOnIOException() throws Exception {
        SseEmitter mockEmitter = mock(SseEmitter.class);
        doThrow(new IOException("disconnected")).when(mockEmitter).send(any(SseEmitter.SseEventBuilder.class));

        getEmitters().add(mockEmitter);
        broadcaster.broadcast(alert());

        assertThat(getEmitters()).doesNotContain(mockEmitter);
    }

    @Test
    void broadcastTransaction_removesEmitterOnIOException() throws Exception {
        SseEmitter mockEmitter = mock(SseEmitter.class);
        doThrow(new IOException("disconnected")).when(mockEmitter).send(any(SseEmitter.SseEventBuilder.class));

        getEmitters().add(mockEmitter);
        broadcaster.broadcastTransaction(scoredTransaction());

        assertThat(getEmitters()).doesNotContain(mockEmitter);
    }

    @Test
    void broadcast_sendsToAllClients() throws Exception {
        SseEmitter first = mock(SseEmitter.class);
        SseEmitter second = mock(SseEmitter.class);
        getEmitters().add(first);
        getEmitters().add(second);

        broadcaster.broadcast(alert());

        verify(first, times(1)).send(any(SseEmitter.SseEventBuilder.class));
        verify(second, times(1)).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void alert_record_fieldsAccessible() {
        AlertBroadcaster.Alert a = alert();
        assertEquals("tx-1", a.transactionId());
        assertEquals("card-1", a.cardId());
        assertEquals("cust-1", a.customerId());
        assertEquals(150.0, a.amount());
        assertEquals("Merchant", a.merchant());
        assertEquals(90, a.riskScore());
        assertEquals("VELOCITY", a.reasonCode());
        assertEquals("Suspicious activity", a.aiExplanation());
    }

    @Test
    void scoredTransaction_record_fieldsAccessible() {
        AlertBroadcaster.ScoredTransaction t = scoredTransaction();
        assertEquals("tx-1", t.transactionId());
        assertEquals("card-1", t.cardId());
        assertEquals(100.0, t.amount());
        assertEquals("Merchant", t.merchant());
        assertEquals(50, t.riskScore());
    }

    // helpers

    private AlertBroadcaster.Alert alert() {
        return new AlertBroadcaster.Alert("tx-1", "card-1", "cust-1", 150.0, "Merchant", 90, "VELOCITY", "Suspicious activity");
    }

    private AlertBroadcaster.ScoredTransaction scoredTransaction() {
        return new AlertBroadcaster.ScoredTransaction("tx-1", "card-1", 100.0, "Merchant", 50);
    }

    @SuppressWarnings("unchecked")
    private List<SseEmitter> getEmitters() throws Exception {
        Field field = AlertBroadcaster.class.getDeclaredField("emitters");
        field.setAccessible(true);
        return (List<SseEmitter>) field.get(broadcaster);
    }
}
