package com.confluent.frauddetectionapp.producer;

import com.confluent.frauddetectionapp.config.KafkaConfig;
import com.devday.frauddetect.avro.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionProducerTest {

    @Mock
    private KafkaTemplate<String, Transaction> kafkaTemplate;

    @Mock
    private KafkaConfig.KafkaTopics topics;

    private CardPool cardPool;
    private TransactionProducer producer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        cardPool = new CardPool();
        when(topics.transactions()).thenReturn("transactions");
        CompletableFuture<SendResult<String, Transaction>> future =
                CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), any(Transaction.class))).thenReturn(future);
        producer = new TransactionProducer(kafkaTemplate, topics, cardPool, 0.02);
    }

    @Test
    void injectVelocityBurst_returnsBetween5And7Ids() {
        List<String> ids = producer.injectVelocityBurst();
        assertThat(ids.size()).isBetween(5, 7);
    }

    @Test
    void injectVelocityBurst_allIdsAreNonNull() {
        List<String> ids = producer.injectVelocityBurst();
        assertThat(ids).doesNotContainNull();
    }

    @Test
    void injectVelocityBurst_allIdsAreUnique() {
        List<String> ids = producer.injectVelocityBurst();
        assertThat(ids).doesNotHaveDuplicates();
    }

    @Test
    void injectVelocityBurst_sendsCorrectNumberOfMessages() {
        List<String> ids = producer.injectVelocityBurst();
        verify(kafkaTemplate, times(ids.size())).send(eq("transactions"), anyString(), any(Transaction.class));
    }

    @Test
    void injectGeoMismatch_returnsExactly2Ids() {
        List<String> ids = producer.injectGeoMismatch();
        assertEquals(2, ids.size());
    }

    @Test
    void injectGeoMismatch_sends2Messages() {
        producer.injectGeoMismatch();
        verify(kafkaTemplate, times(2)).send(eq("transactions"), anyString(), any(Transaction.class));
    }

    @Test
    void injectGeoMismatch_idsAreNonNull() {
        List<String> ids = producer.injectGeoMismatch();
        assertThat(ids).doesNotContainNull();
    }

    @Test
    void injectGeoMismatch_idsAreUnique() {
        List<String> ids = producer.injectGeoMismatch();
        assertThat(ids).doesNotHaveDuplicates();
    }

    @Test
    void generateTransaction_doesNotThrow() {
        assertDoesNotThrow(() -> producer.generateTransaction());
    }

    @Test
    void generateTransaction_sendsAtLeastOneMessage() {
        producer.generateTransaction();
        verify(kafkaTemplate, atLeastOnce()).send(eq("transactions"), anyString(), any(Transaction.class));
    }

    @RepeatedTest(5)
    void injectVelocityBurst_repeatedCalls_allSucceed() {
        assertDoesNotThrow(() -> {
            List<String> ids = producer.injectVelocityBurst();
            assertThat(ids).isNotEmpty();
        });
    }
}
