package dev.kafka.simulator;

public record IndexEntry(
        int relativeOffset,
        int positionBytes
) {
    public IndexEntry {
        if (relativeOffset < 0 || positionBytes < 0) {
            throw new IllegalArgumentException(
                    "Index values must be non-negative"
            );
        }
    }
}
