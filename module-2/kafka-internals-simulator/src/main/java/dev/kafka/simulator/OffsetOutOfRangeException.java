package dev.kafka.simulator;

public class OffsetOutOfRangeException
        extends IllegalArgumentException {

    public OffsetOutOfRangeException(String message) {
        super(message);
    }
}