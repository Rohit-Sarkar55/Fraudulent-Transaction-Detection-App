package com.confluent.frauddetectionapp.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import com.devday.frauddetect.avro.Transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Explicit, typed Kafka beans for the Transaction Avro type.
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
