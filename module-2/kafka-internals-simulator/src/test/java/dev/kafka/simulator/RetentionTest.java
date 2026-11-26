package dev.kafka.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RetentionTest {

    @Test
    void shouldDeleteExpiredClosedSegments() {
        PartitionLog log = newLog();

        log.appendAt(101, "A", 1000);
        log.appendAt(102, "B", 1100);

        log.appendAt(103, "C", 2000);
        log.appendAt(104, "D", 2100);

        log.appendAt(105, "E", 3000);

        assertEquals(3, log.segments().size());

        int deleted = log.deleteExpiredSegments(
                4000, 2000
                                               );

        assertEquals(1, deleted);
        assertEquals(2, log.segments().size());
        assertEquals(2, log.logStartOffset());
        assertEquals(5, log.nextOffset());

        assertEquals(
                List.of(2L, 3L, 4L),
                log.readFrom(2, 10)
                   .stream()
                   .map(SimRecord::offset)
                   .toList()
                    );
    }

    @Test
    void shouldUseMaximumRecordTimestamp() {
        PartitionLog log = newLog();

        log.appendAt(101, "A", 1900);
        log.appendAt(102, "B", 1000);
        log.appendAt(103, "C", 2000);

        assertEquals(
                1900,
                log.segments().getFirst().maxTimestampMs()
                    );

        int deleted = log.deleteExpiredSegments(
                2500, 1000
                                               );

        assertEquals(0, deleted);
        assertEquals(0, log.logStartOffset());
        assertEquals(3, log.nextOffset());
    }

    @Test
    void shouldRejectReadingBeforeLogStartOffset() {
        PartitionLog log = newLog();

        for (int i = 0; i < 5; i++) {
            log.appendAt(101 + i, "A", 1000);
        }

        log.deleteExpiredSegments(5000, 1000);

        assertEquals(4, log.logStartOffset());

        assertThrows(
                OffsetOutOfRangeException.class,
                () -> log.readFrom(0, 10)
                    );

        assertEquals(
                List.of(4L),
                log.readFrom(4, 10)
                   .stream()
                   .map(SimRecord::offset)
                   .toList()
                    );
    }

    @Test
    void shouldDeleteOldSegmentsBySize() {
        PartitionLog log = newLog();

        for (int i = 0; i < 5; i++) {
            log.appendAt(101 + i, "A", 1000 + i);
        }

        // Каждая запись: 24 + 1 = 25 bytes.
        // Сегменты: 50, 50, 25.
        assertEquals(125, log.totalSizeBytes());

        int deleted = log.enforceRetentionBytes(80);

        assertEquals(1, deleted);
        assertEquals(75, log.totalSizeBytes());
        assertEquals(2, log.logStartOffset());
        assertEquals(5, log.nextOffset());
    }

    @Test
    void shouldNeverDeleteActiveSegment() {
        PartitionLog log = newLog();

        log.appendAt(101, "A", 1000);

        assertEquals(
                0,
                log.deleteExpiredSegments(10000, 100)
                    );

        assertEquals(
                0,
                log.enforceRetentionBytes(1)
                    );

        assertEquals(1, log.segments().size());
        assertEquals(0, log.logStartOffset());
        assertEquals(1, log.nextOffset());
    }

    @Test
    void shouldContinueOffsetsAfterRetention() {
        PartitionLog log = newLog();

        log.appendAt(101, "A", 1000);
        log.appendAt(102, "B", 1100);

        log.appendAt(103, "C", 2000);
        log.appendAt(104, "D", 2100);

        log.appendAt(105, "E", 3000);

        assertEquals(3, log.segments().size());

        int deleted = log.deleteExpiredSegments(
                4000, 2000
                                               );

        assertEquals(1, deleted);
        assertEquals(2, log.segments().size());
        assertEquals(2, log.logStartOffset());
        assertEquals(5, log.nextOffset());
        long no = log.nextOffset();

        SimRecord record = log.appendAt(106, "H", 5000);
        assertEquals(no, record.offset());
    }

    @Test
    void shouldHandleEmptyLogRetention() {
        PartitionLog partitionLog = newLog();
        partitionLog.deleteExpiredSegments(System.currentTimeMillis(), 100);
        partitionLog.enforceRetentionBytes(100);
        assertEquals(0, partitionLog.logStartOffset());
        assertEquals(0, partitionLog.nextOffset());
        assertEquals(1, partitionLog.segments().size());
    }

    @Test
    void shouldRejectInvalidRetentionArguments() {
        PartitionLog partitionLog = newLog();
        partitionLog.appendAt(101L, "A", 1000);
        partitionLog.appendAt(101L, "B", 2000);
        assertThrows(IllegalArgumentException.class,
                     () -> partitionLog.deleteExpiredSegments(System.currentTimeMillis(), -10));
        assertThrows(IllegalArgumentException.class, () -> partitionLog.enforceRetentionBytes(-101));
        assertThrows(
                IllegalArgumentException.class,
                () -> partitionLog.deleteExpiredSegments(-1, 100)
                    );

        assertEquals(0, partitionLog.logStartOffset());
        assertEquals(2, partitionLog.nextOffset());
        assertEquals(1, partitionLog.segments().size());
        assertEquals(50, partitionLog.totalSizeBytes());
    }

    @Test
    void shouldPreserveSparseIndexAfterRetention() {
        PartitionLog log = new PartitionLog(0, 3, 50);

        // Segment 0.
        log.appendAt(101, "A", 1000);
        log.appendAt(102, "B", 1100);
        log.appendAt(103, "C", 1200);

        // Segment 3.
        log.appendAt(104, "X".repeat(30), 3000);
        log.appendAt(105, "A", 3100);
        log.appendAt(106, "BB", 3200);

        // Segment 6 (active).
        log.appendAt(107, "C", 3300);

        List<IndexEntry> expectedIndex = List.of(
                new IndexEntry(0, 0),
                new IndexEntry(1, 54)
                                                );

        assertEquals(
                expectedIndex,
                log.segments().get(1).indexEntries()
                    );

        assertEquals(
                1,
                log.deleteExpiredSegments(4000, 2000)
                    );

        assertEquals(3, log.logStartOffset());

        assertEquals(
                expectedIndex,
                log.segments().getFirst().indexEntries()
                    );

        ReadResult result = log.readFromWithStats(4, 2);

        assertEquals(
                List.of(4L, 5L),
                result.records().stream()
                      .map(SimRecord::offset)
                      .toList()
                    );

        assertEquals(2, result.scannedRecords());
    }

    @Test
    void shouldStopRetentionAtFirstNonExpiredSegment() {
        PartitionLog log = newLog();

        // Segment 0 — expired.
        log.appendAt(101, "A", 1000);
        log.appendAt(102, "B", 1000);

        // Segment 2 — not expired.
        log.appendAt(103, "C", 5000);
        log.appendAt(104, "D", 5000);

        // Segment 4 — expired.
        log.appendAt(105, "E", 2000);
        log.appendAt(106, "F", 2000);

        // Segment 6 — active.
        log.appendAt(107, "G", 6000);

        int deleted = log.deleteExpiredSegments(
                7000, 3000
                                               );

        assertEquals(1, deleted);
        assertEquals(2, log.logStartOffset());
        assertEquals(7, log.nextOffset());

        assertEquals(
                List.of(2L, 4L, 6L),
                log.segments().stream()
                   .map(LogSegment::baseOffset)
                   .toList()
                    );

        assertEquals(
                List.of(2L, 3L, 4L, 5L, 6L),
                log.readFrom(2, 10).stream()
                   .map(SimRecord::offset)
                   .toList()
                    );
    }

    @Test
    void shouldNotRollSegmentOnInvalidAppend() {
        PartitionLog log = newLog();

        log.appendAt(101, "A", 1000);
        log.appendAt(102, "B", 2000);

        assertThrows(
                IllegalArgumentException.class,
                () -> log.appendAt(103, "C", -1)
                    );

        assertEquals(1, log.segments().size());
        assertEquals(2, log.nextOffset());
        assertEquals(0, log.logStartOffset());
        assertEquals(50, log.totalSizeBytes());
        assertEquals(2000, log.segments().getFirst().maxTimestampMs());
    }

    @Test
    void shouldAcceptZeroTimestamp() {
        PartitionLog log = newLog();

        SimRecord record = log.appendAt(101, "A", 0);

        assertEquals(0, record.offset());
        assertEquals(1, log.nextOffset());
        assertEquals(0, log.segments().getFirst().maxTimestampMs());
    }

    private PartitionLog newLog() {
        return new PartitionLog(0, 2, 50);
    }
}
