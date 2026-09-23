package com.confluent.frauddetectionapp.producer;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A small, fixed pool of fake cards and their "home" profile.
 *
 * Real fraud signals (velocity, geo-mismatch, amount outliers) only make
 * sense when the SAME card/customer shows up across multiple transactions
 * over time. A pool of fully random IDs on every transaction would never
 * trigger any of the Flink checks, so instead we keep a small recurring
 * cast of fake cards, each with a stable home location and average spend.
 */
@Component
public class CardPool {

    public record Card(
            String cardId,
            String customerId,
            double homeLat,
            double homeLon,
            String homeCity,
            double avgSpend) {
    }

    // A handful of cities to place "home" locations and to draw
    // geo-mismatch fraud from (far apart, so impossible-travel is obvious).
    private static final List<double[]> CITIES = List.of(
            new double[]{40.7128, -74.0060},   // New York
            new double[]{51.5074, -0.1278},    // London
            new double[]{35.6762, 139.6503},   // Tokyo
            new double[]{6.5244, 3.3792},      // Lagos
            new double[]{52.5200, 13.4050},    // Berlin
            new double[]{-33.8688, 151.2093},  // Sydney
            new double[]{19.4326, -99.1332}    // Mexico City
    );

    private static final List<String> CITY_NAMES = List.of(
            "New York", "London", "Tokyo", "Lagos", "Berlin", "Sydney", "Mexico City"
    );

    private final List<Card> cards;

    public CardPool() {
        this.cards = buildPool(50);
    }

    private List<Card> buildPool(int size) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return java.util.stream.IntStream.range(0, size)
                .mapToObj(i -> {
                    int cityIdx = rnd.nextInt(CITIES.size());
                    double[] home = CITIES.get(cityIdx);
                    return new Card(
                            String.valueOf(1000 + rnd.nextInt(9000)),
                            "cust-" + (100 + i),
                            home[0],
                            home[1],
                            CITY_NAMES.get(cityIdx),
                            20 + rnd.nextDouble() * 180 // avg spend $20-$200
                    );
                })
                .toList();
    }

    public Card random() {
        return cards.get(ThreadLocalRandom.current().nextInt(cards.size()));
    }

    /** A location far from the card's home city, for geo-mismatch injection. */
    public double[] farAwayLocation(Card card) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        double[] farCity;
        do {
            farCity = CITIES.get(rnd.nextInt(CITIES.size()));
        } while (farCity[0] == card.homeLat() && farCity[1] == card.homeLon());
        return farCity;
    }

    public List<Card> all() {
        return cards;
    }
}