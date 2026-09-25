package com.confluent.frauddetectionapp.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class CardPoolTest {

    private CardPool cardPool;

    @BeforeEach
    void setUp() {
        cardPool = new CardPool();
    }

    @Test
    void all_returns50Cards() {
        assertThat(cardPool.all()).hasSize(50);
    }

    @Test
    void random_returnsNonNull() {
        CardPool.Card card = cardPool.random();
        assertNotNull(card);
    }

    @Test
    void random_returnsCardFromPool() {
        CardPool.Card card = cardPool.random();
        assertThat(cardPool.all()).contains(card);
    }

    @Test
    void cards_haveValidFields() {
        for (CardPool.Card card : cardPool.all()) {
            assertNotNull(card.cardId());
            assertNotNull(card.customerId());
            assertNotNull(card.homeCity());
            assertThat(card.avgSpend()).isBetween(20.0, 200.0);
        }
    }

    @Test
    void cards_haveUniqueCustomerIds() {
        List<String> customerIds = cardPool.all().stream().map(CardPool.Card::customerId).toList();
        assertThat(customerIds).doesNotHaveDuplicates();
    }

    @Test
    void farAwayLocation_returnsTwoElementArray() {
        CardPool.Card card = new CardPool.Card("1234", "cust-1", 40.7128, -74.0060, "New York", 100.0);
        double[] location = cardPool.farAwayLocation(card);
        assertThat(location).hasSize(2);
    }

    @RepeatedTest(10)
    void farAwayLocation_neverReturnsHomeLocation() {
        CardPool.Card card = new CardPool.Card("1234", "cust-1", 40.7128, -74.0060, "New York", 100.0);
        double[] location = cardPool.farAwayLocation(card);
        boolean sameAsHome = location[0] == card.homeLat() && location[1] == card.homeLon();
        assertFalse(sameAsHome, "Far location must differ from home");
    }

    @Test
    void card_recordFieldsAreAccessible() {
        CardPool.Card card = new CardPool.Card("9999", "cust-99", 51.5074, -0.1278, "London", 75.5);
        assertEquals("9999", card.cardId());
        assertEquals("cust-99", card.customerId());
        assertEquals(51.5074, card.homeLat());
        assertEquals(-0.1278, card.homeLon());
        assertEquals("London", card.homeCity());
        assertEquals(75.5, card.avgSpend());
    }
}
