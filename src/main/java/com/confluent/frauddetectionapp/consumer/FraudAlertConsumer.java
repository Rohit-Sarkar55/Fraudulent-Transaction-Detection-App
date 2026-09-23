package com.confluent.frauddetectionapp.consumer;

import com.confluent.frauddetectionapp.service.AlertBroadcaster;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to fraud_alerts_explained (the final, AI-annotated alert
 * stream) and forwards each alert to the browser dashboard via SSE.
 *
 * Deserializes as GenericRecord rather than a generated Java class,
 * since this topic's schema isn't tied to a compiled Avro class the
 * way Transaction is -- it was created directly in Flink SQL, not
 * from an .avsc file in this project.
 */
@Component
public class FraudAlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertConsumer.class);

    private final AlertBroadcaster broadcaster;

    public FraudAlertConsumer(AlertBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.fraud-alerts-explained}",
            groupId = "fraud-dashboard-v2",
            containerFactory = "genericRecordListenerFactory"
    )
    public void onAlert(GenericRecord record) {
        try {
            var alert = new AlertBroadcaster.Alert(
                    str(record, "transaction_id"),
                    str(record, "card_id"),
                    str(record, "customer_id"),
                    doubleVal(record, "amount"),
                    str(record, "merchant"),
                    (Integer) record.get("risk_score"),
                    str(record, "reason_code"),
                    str(record, "ai_explanation")
            );
            log.info("Fraud alert received: card={} risk_score={}", alert.cardId(), alert.riskScore());
            broadcaster.broadcast(alert);
        } catch (Exception e) {
            log.error("Failed to process fraud alert record", e);
        }
    }

    private String str(GenericRecord record, String field) {
        Object value = record.get(field);
        return value == null ? null : value.toString();
    }

    private double doubleVal(GenericRecord record, String field) {
        Object value = record.get(field);
        return value == null ? 0.0 : ((Number) value).doubleValue();
    }
}