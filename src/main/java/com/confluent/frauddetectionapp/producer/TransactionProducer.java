package com.confluent.frauddetectionapp.producer;

import com.devday.frauddetect.avro.Transaction;
import com.confluent.frauddetectionapp.config.KafkaConfig.KafkaTopics;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates synthetic card transactions and publishes them to the
 * 'transactions' topic.
 *
 * Most transactions are ordinary. A small percentage (app.fraud.injection-rate)
 * are deliberately suspicious, using one of three patterns Flink is set up
 * to catch: velocity, amount outlier, or geo-mismatch (impossible travel).
 * A manual burst can also be triggered on demand via DemoController, for a
 * guaranteed alert during a live demo.
 */
@Component
public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final KafkaTopics topics;
    private final CardPool cardPool;
    private final Faker faker = new Faker();
    private final double injectionRate;

    public TransactionProducer(
            KafkaTemplate<String, Transaction> kafkaTemplate,
            KafkaTopics topics,
            CardPool cardPool,
            @Value("${app.fraud.injection-rate}") double injectionRate) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.cardPool = cardPool;
        this.injectionRate = injectionRate;
    }

    /** Runs continuously: ~6 transactions/minute (one every 10 seconds), mostly normal.
     * Kept slow so the topic doesn't flood during a demo -- use the
     * manual injection endpoints (FraudInjectionController) to force
     * activity on demand instead of waiting on this background rate. */
    @Scheduled(fixedRate = 10000)
    public void generateTransaction() {
        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < injectionRate / 3) {
            injectVelocityBurst();
        } else if (roll < injectionRate * 2 / 3) {
            injectGeoMismatch();
        } else if (roll < injectionRate) {
            send(buildAmountOutlier(cardPool.random()));
        } else {
            send(buildNormalTransaction(cardPool.random()));
        }
    }

    // ---- normal traffic ----

    private Transaction buildNormalTransaction(CardPool.Card card) {
        double amount = round2(card.avgSpend() * (0.5 + ThreadLocalRandom.current().nextDouble()));
        return build(card, amount, card.homeLat(), card.homeLon());
    }

    // ---- fraud patterns ----

    /** Same card, several transactions in rapid succession -> velocity check in Flink. */
    public List<String> injectVelocityBurst() {
        CardPool.Card card = cardPool.random();
        int burstSize = 5 + ThreadLocalRandom.current().nextInt(3); // 5-7 rapid txns
        log.info("Injecting velocity burst: card={} count={}", card.cardId(), burstSize);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < burstSize; i++) {
            double amount = round2(15 + ThreadLocalRandom.current().nextDouble() * 60);
            Transaction transaction = build(card, amount, card.homeLat(), card.homeLon());
            ids.add(transaction.getTransactionId());
            send(transaction);
        }
        return ids;
    }

    /** Same card, a location far from home right after a normal one -> impossible travel. */
    public List<String> injectGeoMismatch() {
        CardPool.Card card = cardPool.random();
        double[] farLocation = cardPool.farAwayLocation(card);
        log.info("Injecting geo-mismatch: card={} homeCity={}", card.cardId(), card.homeCity());
        List<String> ids = new ArrayList<>();
        Transaction normal = buildNormalTransaction(card);
        ids.add(normal.getTransactionId());
        send(normal);
        Transaction farAway = build(card, round2(card.avgSpend()), farLocation[0], farLocation[1]);
        ids.add(farAway.getTransactionId());
        send(farAway);
        return ids;
    }

    /** One transaction far above the card's normal spend -> amount anomaly. */
    private Transaction buildAmountOutlier(CardPool.Card card) {
        double amount = round2(card.avgSpend() * (8 + ThreadLocalRandom.current().nextDouble() * 6));
        log.info("Injecting amount outlier: card={} amount={}", card.cardId(), amount);
        return build(card, amount, card.homeLat(), card.homeLon());
    }

    // ---- shared build/send ----

    private Transaction build(CardPool.Card card, double amount, double lat, double lon) {
        return Transaction.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setCardId(card.cardId())
                .setCustomerId(card.customerId())
                .setAmount(amount)
                .setMerchant(faker.company().name())
                .setLat(lat)
                .setLon(lon)
                .setTimestamp(Instant.now())
                .setDeviceId("device-" + UUID.randomUUID().toString().substring(0, 8))
                .build();
    }

    private void send(Transaction transaction) {
        log.info("Sending transaction_id={} card={} amount={}",
                transaction.getTransactionId(), transaction.getCardId(), transaction.getAmount());
        kafkaTemplate.send(topics.transactions(), transaction.getCardId(), transaction)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send transaction {}", transaction.getTransactionId(), ex);
                    }
                });
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}