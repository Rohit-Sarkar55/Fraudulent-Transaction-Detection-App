package com.confluent.frauddetectionapp.config;

import com.devday.frauddetect.avro.Transaction;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Explicit, typed Kafka beans for the Transaction Avro type, plus a
 * GenericRecord-based consumer factory for reading topics (like
 * fraud_alerts_explained) that were created directly in Flink SQL
 * rather than from a compiled Avro class in this project.
 *
 * Spring Boot autoconfigures a generic KafkaTemplate from
 * application.properties, but declaring this bean explicitly means
 * TransactionProducer can inject KafkaTemplate<String, Transaction>
 * directly instead of casting a generic Object-typed template.
 */
@Configuration
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ProducerFactory<String, Transaction> transactionProducerFactory() {
        // Start from the properties Spring Boot already parsed out of
        // application.properties (bootstrap servers, SASL, schema registry
        // auth) so we don't repeat that config here.
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Transaction> transactionKafkaTemplate() {
        return new KafkaTemplate<>(transactionProducerFactory());
    }

    // GenericRecord consumer factory: for topics created in Flink SQL
    // (fraud_alerts_explained, scored_transactions) that don't have a
    // compiled Java class. Used only by the dashboard's consumers, so
    // auto.offset.reset is forced to 'latest' here -- overriding the
    // 'earliest' default in application.properties -- meaning the
    // dashboard only ever shows alerts/transactions from the moment
    // it starts, not a replay of everything ever produced during
    // earlier testing.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> genericRecordListenerFactory() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("specific.avro.reader", false); // GenericRecord, not a compiled class
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        var factory = new ConcurrentKafkaListenerContainerFactory<String, GenericRecord>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        return factory;
    }

    // Topic names, injected from application.properties so they're not
    // hardcoded across the codebase.
    @Bean
    public KafkaTopics kafkaTopics(
            @Value("${app.kafka.topics.transactions}") String transactions,
            @Value("${app.kafka.topics.customer-profiles}") String customerProfiles,
            @Value("${app.kafka.topics.scored-transactions}") String scoredTransactions,
            @Value("${app.kafka.topics.fraud-alerts}") String fraudAlerts) {
        return new KafkaTopics(transactions, customerProfiles, scoredTransactions, fraudAlerts);
    }

    public record KafkaTopics(
            String transactions,
            String customerProfiles,
            String scoredTransactions,
            String fraudAlerts) {
    }
}