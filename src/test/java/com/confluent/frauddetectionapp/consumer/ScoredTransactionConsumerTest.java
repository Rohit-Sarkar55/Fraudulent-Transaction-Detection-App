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
class ScoredTransactionConsumerTest {

    @Mock
    private AlertBroadcaster broadcaster;

    private ScoredTransactionConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new ScoredTransactionConsumer(broadcaster);
    }

    @Test
    void onScoredTransaction_withValidRecord_broadcastsTransaction() {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get("transaction_id")).thenReturn("tx-001");
        when(record.get("card_id")).thenReturn("card-999");
        when(record.get("amount")).thenReturn(200.50);
        when(record.get("merchant")).thenReturn("BestBuy");
        when(record.get("risk_score")).thenReturn(30);

        consumer.onScoredTransaction(record);

        ArgumentCaptor<AlertBroadcaster.ScoredTransaction> captor =
                ArgumentCaptor.forClass(AlertBroadcaster.ScoredTransaction.class);
        verify(broadcaster).broadcastTransaction(captor.capture());

        AlertBroadcaster.ScoredTransaction txn = captor.getValue();
        assertEquals("tx-001", txn.transactionId());
        assertEquals("card-999", txn.cardId());
        assertEquals(200.50, txn.amount());
        assertEquals("BestBuy", txn.merchant());
        assertEquals(30, txn.riskScore());
    }

    @Test
    void onScoredTransaction_withNullFields_usesDefaults() {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get("transaction_id")).thenReturn(null);
        when(record.get("card_id")).thenReturn(null);
        when(record.get("amount")).thenReturn(null);
        when(record.get("merchant")).thenReturn(null);
        when(record.get("risk_score")).thenReturn(null);

        consumer.onScoredTransaction(record);

        ArgumentCaptor<AlertBroadcaster.ScoredTransaction> captor =
                ArgumentCaptor.forClass(AlertBroadcaster.ScoredTransaction.class);
        verify(broadcaster).broadcastTransaction(captor.capture());

        AlertBroadcaster.ScoredTransaction txn = captor.getValue();
        assertNull(txn.transactionId());
        assertNull(txn.cardId());
        assertEquals(0.0, txn.amount());
        assertNull(txn.merchant());
        assertEquals(0, txn.riskScore());
    }

    @Test
    void onScoredTransaction_whenExceptionThrown_doesNotPropagate() {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get(anyString())).thenThrow(new RuntimeException("parse error"));

        assertDoesNotThrow(() -> consumer.onScoredTransaction(record));
        verify(broadcaster, never()).broadcastTransaction(any());
    }

    @Test
    void onScoredTransaction_withFloatAmount_convertsToDouble() {
        GenericRecord record = mock(GenericRecord.class);
        when(record.get("transaction_id")).thenReturn("tx-float");
        when(record.get("card_id")).thenReturn("card-1");
        when(record.get("amount")).thenReturn(49.99f);
        when(record.get("merchant")).thenReturn("Amazon");
        when(record.get("risk_score")).thenReturn(15);

        consumer.onScoredTransaction(record);

        ArgumentCaptor<AlertBroadcaster.ScoredTransaction> captor =
                ArgumentCaptor.forClass(AlertBroadcaster.ScoredTransaction.class);
        verify(broadcaster).broadcastTransaction(captor.capture());
        assertEquals(49.99f, captor.getValue().amount(), 0.001);
    }
}
