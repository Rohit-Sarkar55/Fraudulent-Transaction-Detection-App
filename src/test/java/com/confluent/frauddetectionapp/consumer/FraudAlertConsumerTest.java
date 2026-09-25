package com.confluent.frauddetectionapp.consumer;

import com.confluent.frauddetectionapp.service.AlertBroadcaster;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FraudAlertConsumerTest {

    @Mock
    private AlertBroadcaster broadcaster;

    private FraudAlertConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new FraudAlertConsumer(broadcaster);
    }

    @Test
    void onAlert_withValidRecord_broadcastsAlert() {
        GenericRecord record = mockRecord("tx-123", "card-456", "cust-789", 150.0, "Acme Corp", 95, "VELOCITY", "Rapid transactions");

        consumer.onAlert(record);

        ArgumentCaptor<AlertBroadcaster.Alert> captor = ArgumentCaptor.forClass(AlertBroadcaster.Alert.class);
        verify(broadcaster).broadcast(captor.capture());

        AlertBroadcaster.Alert alert = captor.getValue();
        assertEquals("tx-123", alert.transactionId());
        assertEquals("card-456", alert.cardId());
        assertEquals("cust-789", alert.customerId());
        assertEquals(150.0, alert.amount());
        assertEquals("Acme Corp", alert.merchant());
        assertEquals(95, alert.riskScore());
        assertEquals("VELOCITY", alert.reasonCode());
        assertEquals("Rapid transactions", alert.aiExplanation());
    }

    @Test
    void onAlert_withNullFields_broadcastsAlertWithNullValues() {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get("transaction_id")).thenReturn(null);
        when(record.get("card_id")).thenReturn(null);
        when(record.get("customer_id")).thenReturn(null);
        when(record.get("amount")).thenReturn(null);
        when(record.get("merchant")).thenReturn(null);
        when(record.get("risk_score")).thenReturn(42);
        when(record.get("reason_code")).thenReturn(null);
        when(record.get("ai_explanation")).thenReturn(null);

        consumer.onAlert(record);

        ArgumentCaptor<AlertBroadcaster.Alert> captor = ArgumentCaptor.forClass(AlertBroadcaster.Alert.class);
        verify(broadcaster).broadcast(captor.capture());

        AlertBroadcaster.Alert alert = captor.getValue();
        assertNull(alert.transactionId());
        assertNull(alert.cardId());
        assertEquals(0.0, alert.amount());
    }

    @Test
    void onAlert_whenExceptionThrown_doesNotPropagate() {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get(anyString())).thenThrow(new RuntimeException("Avro error"));

        assertDoesNotThrow(() -> consumer.onAlert(record));
        verify(broadcaster, never()).broadcast(any());
    }

    @Test
    void onAlert_withNonStandardAmountType_handledAsNumber() {
        GenericRecord record = mockRecord("tx-1", "card-1", "cust-1", 99.99f, "Shop", 80, "GEO", "Far location");

        consumer.onAlert(record);

        ArgumentCaptor<AlertBroadcaster.Alert> captor = ArgumentCaptor.forClass(AlertBroadcaster.Alert.class);
        verify(broadcaster).broadcast(captor.capture());
        assertEquals(99.99f, captor.getValue().amount(), 0.001);
    }

    // helper that builds a mock GenericRecord
    private GenericRecord mockRecord(String txId, String cardId, String custId,
                                     Number amount, String merchant,
                                     Integer riskScore, String reasonCode, String explanation) {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get("transaction_id")).thenReturn(txId);
        when(record.get("card_id")).thenReturn(cardId);
        when(record.get("customer_id")).thenReturn(custId);
        when(record.get("amount")).thenReturn(amount);
        when(record.get("merchant")).thenReturn(merchant);
        when(record.get("risk_score")).thenReturn(riskScore);
        when(record.get("reason_code")).thenReturn(reasonCode);
        when(record.get("ai_explanation")).thenReturn(explanation);
        return record;
    }
}
