package dev.kafka.simulator;

public record SimRecord(int partition,
                        long offset,
                        long orderId,
                        String eventType) {
}
