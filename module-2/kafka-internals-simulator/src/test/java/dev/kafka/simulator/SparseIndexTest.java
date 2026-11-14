package dev.kafka.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseIndexTest {

    @Test
    void shouldFindFloorIndexEntry() {
        OffsetIndex index = new OffsetIndex();

        index.append(0, 0);
        index.append(2, 50);
        index.append(4, 100);

        assertEquals(
                new IndexEntry(0, 0),
                index.floor(1)
                    );

        assertEquals(
                new IndexEntry(2, 50),
                index.floor(3)
                    );

        assertEquals(
                new IndexEntry(4, 100),
                index.floor(5)
                    );

        assertNull(index.floor(-1));
    }

    @Test
    void shouldCreateSparseEntriesByByteInterval() {
        LogSegment segment = new LogSegment(100, 50);

        for (int i = 0; i < 6; i++) {
            segment.append(
                    new SimRecord(0, 100L + i, 101L, "A")
                          );
        }

        assertEquals(
                List.of(
                        new IndexEntry(0, 0),
                        new IndexEntry(2, 50),
                        new IndexEntry(4, 100)
                       ),
                segment.indexEntries()
                    );

        assertEquals(150, segment.sizeBytes());
    }

    @Test
    void shouldReadFromNearestIndexedPosition() {
        LogSegment segment = new LogSegment(100, 50);

        for (int i = 0; i < 6; i++) {
            segment.append(
                    new SimRecord(0, 100L + i, 101L, "A")
                          );
        }

        ReadResult result = segment.readIndexed(103, 2);

        assertEquals(
                List.of(103L, 104L),
                result.records().stream()
                      .map(SimRecord::offset)
                      .toList()
                    );

        assertEquals(3, result.scannedRecords());
    }

    @Test
    void shouldReadAcrossMultipleSegments() {
        PartitionLog log = new PartitionLog(0, 3, 50);

        for (int i = 0; i < 8; i++) {
            log.append(101L, "A");
        }

        assertEquals(
                List.of(2L, 3L, 4L, 5L),
                log.readFrom(2, 4).stream()
                   .map(SimRecord::offset)
                   .toList()
                    );
    }


    @Test
    void shouldReadWhenOffsetsHaveGaps() {
        LogSegment segment = new LogSegment(100, 50);

        segment.append(new SimRecord(0, 100L, 101L, "A"));
        segment.append(new SimRecord(0, 103L, 101L, "B"));
        segment.append(new SimRecord(0, 105L, 101L, "C"));

        ReadResult result = segment.readIndexed(101, 10);

        assertEquals(
                List.of(103L, 105L),
                result.records().stream()
                      .map(SimRecord::offset)
                      .toList()
                    );
    }

    @Test
    void shouldValidationFailed() {
        OffsetIndex oi = new OffsetIndex();
        assertThrows(IllegalArgumentException.class, () -> oi.append(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> oi.append(2, -1));

        oi.append(0, 15);
        oi.append(1, 25);
        assertThrows(IllegalStateException.class, () -> oi.append(1, 30));
        assertThrows(IllegalStateException.class, () -> oi.append(2, 20));
    }

    @Test
    void shouldReadEmptyAfterLastOffset() {
        LogSegment segment = new LogSegment(100, 50);

        for (int i = 0; i < 6; i++) {
            segment.append(new SimRecord(0, 100L + i, 101L, "A"));
        }

        ReadResult result = segment.readIndexed(106, 10);
        assertTrue(result.records().isEmpty());
        assertEquals(2, result.scannedRecords());
    }

    @Test
    void shouldEvaluateSizeByUtf8() {
        LogSegment segment = new LogSegment(0, 50);

        segment.append(
                new SimRecord(0, 0, 1, "Я")
                      );

        assertEquals(26, segment.sizeBytes());
    }

    @Test
    void shouldBeEffectiveWithIndex() {
        LogSegment segment = new LogSegment(100, 50);
        for (int i = 0; i < 6; i++) {
            segment.append(new SimRecord(0, 100L + i, 101L, "A"));
        }

        ReadResult readResult = segment.readIndexed(104, 1);
        assertEquals(1, readResult.records().size());
        assertEquals(1, readResult.scannedRecords());
    }


    @Test
    void shouldIndexUsingStartBytePositions() {
        LogSegment segment = new LogSegment(0, 50);

        segment.append(new SimRecord(0, 0, 101, "A"));
        segment.append(new SimRecord(0, 1, 101, "X".repeat(60)));
        segment.append(new SimRecord(0, 2, 101, "B"));

        assertEquals(
                List.of(
                        new IndexEntry(0, 0),
                        new IndexEntry(2, 109)
                       ),
                segment.indexEntries()
                    );
    }


    @Test
    void shouldRejectDecreasingOffsets() {
        LogSegment segment = new LogSegment(100, 50);

        segment.append(new SimRecord(0, 100, 101, "A"));
        segment.append(new SimRecord(0, 105, 101, "B"));

        assertThrows(
                IllegalArgumentException.class,
                () -> segment.append(
                        new SimRecord(0, 103, 101, "C")
                                    )
                    );
    }


    @Test
    void shouldKeepIndexIntervalAfterSegmentRoll() {
        PartitionLog log = new PartitionLog(0, 2, 100);

        log.append(101, "A");
        log.append(101, "B");
        log.append(101, "C");
        log.append(101, "D");

        assertEquals(
                List.of(new IndexEntry(0, 0)),
                log.segments().get(1).indexEntries()
                    );
    }

    @Test
    void shouldReadEmptyPartition() {
        PartitionLog log = new PartitionLog(0, 5, 50);

        ReadResult result = log.readFromWithStats(0, 10);

        assertTrue(result.records().isEmpty());
        assertEquals(0, result.scannedRecords());
    }
}
