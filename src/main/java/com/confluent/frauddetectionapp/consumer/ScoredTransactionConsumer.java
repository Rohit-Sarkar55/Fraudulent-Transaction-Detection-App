package com.confluent.frauddetectionapp.consumer;

import com.confluent.frauddetectionapp.service.AlertBroadcaster;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to EVERY scored transaction (not just the ones that crossed
 * the fraud threshold) and forwards a lightweight summary to the
 * dashboard's transaction feed table, so the demo shows a realistic
 * mix of Approved/Review/Blocked traffic rather than only alerts.
 */
@Component
public class ScoredTransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(ScoredTransactionConsumer.class);

    private final AlertBroadcaster broadcaster;

    public ScoredTransactionConsumer(AlertBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.scored-transactions}",
            groupId = "fraud-dashboard-transactions-v2",
            containerFactory = "genericRecordListenerFactory"
    )
    public void onScoredTransaction(GenericRecord record) {
        try {
            Object txId = record.get("transaction_id");
            Object cardId = record.get("card_id");
            Object amount = record.get("amount");
            Object merchant = record.get("merchant");
            Object riskScore = record.get("risk_score");

            broadcaster.broadcastTransaction(new AlertBroadcaster.ScoredTransaction(
                    txId == null ? null : txId.toString(),
                    cardId == null ? null : cardId.toString(),
                    amount == null ? 0.0 : ((Number) amount).doubleValue(),
                    merchant == null ? null : merchant.toString(),
                    riskScore == null ? 0 : (Integer) riskScore
            ));
        } catch (Exception e) {
            log.error("Failed to process scored transaction record", e);
        }
    }
}