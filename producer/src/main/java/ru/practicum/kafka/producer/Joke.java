package ru.practicum.kafka.producer;

public record Joke(
        int id,
        String category,
        String setup,
        String delivery
) {
}
