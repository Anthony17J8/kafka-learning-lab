package dev.kafka.simulator;

import java.util.List;

public record ReadResult(
        List<SimRecord> records,
        int scannedRecords
) {
    public ReadResult {
        records = List.copyOf(records);
    }
}
