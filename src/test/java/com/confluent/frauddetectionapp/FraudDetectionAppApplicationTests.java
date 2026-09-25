package com.confluent.frauddetectionapp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class FraudDetectionAppApplicationTests {

    @Test
    void applicationClassInstantiable() {
        assertNotNull(new FraudDetectionAppApplication());
    }
}
