package dev.kafka.simulator;

public enum TransactionStatus {
    OPEN,
    COMMITTING,
    COMMITTED,
    ABORTING,
    ABORTED
}
