package dev.kafka.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopicTest {

    @Test
    void shouldRouteRecordsToPartitionsByKey() {
        Topic topic = new Topic(3, 2);

        SimRecord a = topic.publish(101L, "Created");
        SimRecord b = topic.publish(102L, "Created");
        SimRecord c = topic.publish(103L, "Created");

        assertEquals(2, a.partition());
        assertEquals(0, b.partition());
        assertEquals(1, c.partition());
    }

    @Test
    void shouldAssignIndependentOffsets() {
        Topic topic = new Topic(3, 2);

        SimRecord a = topic.publish(101L, "Created");
        SimRecord b = topic.publish(102L, "Created");
        SimRecord c = topic.publish(101L, "Reserved");

        assertEquals(0L, a.offset());
        assertEquals(0L, b.offset());
        assertEquals(1L, c.offset());

        assertEquals(a.partition(), c.partition());
    }

    @Test
    void shouldRollSegmentOnlyWhenFull() {
        PartitionLog log = new PartitionLog(0, 2);

        log.append(101L, "Created");
        log.append(101L, "Reserved");

        assertEquals(1, log.segments().size());

        log.append(101L, "Paid");

        assertEquals(2, log.segments().size());
        assertEquals(0L, log.segments().get(0).baseOffset());
        assertEquals(2L, log.segments().get(1).baseOffset());
    }

    @Test
    void shouldKeepOffsetsAcrossSegments() {
        PartitionLog log = new PartitionLog(0, 2);

        for (int i = 0; i < 5; i++) {
            log.append(101L, "Event-" + i);
        }

        assertEquals(5L, log.nextOffset());

        assertEquals(
                List.of(0L, 2L, 4L),
                log.segments().stream()
                   .map(LogSegment::baseOffset)
                   .toList()
                    );

        assertEquals(
                List.of(2, 2, 1),
                log.segments().stream()
                   .map(LogSegment::size)
                   .toList()
                    );
    }

    @Test
    void shouldReadFromOffsetWithLimit() {
        PartitionLog log = new PartitionLog(0, 2);

        for (int i = 0; i < 5; i++) {
            log.append(101L, "Event-" + i);
        }

        List<Long> result = log.readFrom(2, 2)
                               .stream()
                               .map(SimRecord::offset)
                               .toList();

        assertEquals(List.of(2L, 3L), result);

        assertEquals(
                List.of(4L),
                log.readFrom(4, 10).stream()
                   .map(SimRecord::offset)
                   .toList()
                    );

        assertTrue(log.readFrom(5, 10).isEmpty());
    }

    @Test
    void shouldRejectInvalidArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionLog(0, 0)
                    );

        assertThrows(
                IllegalArgumentException.class,
                () -> new Topic(0, 2)
                    );

        PartitionLog log = new PartitionLog(0, 2);

        assertThrows(
                IllegalArgumentException.class,
                () -> log.readFrom(0, 0)
                    );
    }

    @Test
    void singleFullSegment() {
        PartitionLog log = new PartitionLog(0, 5);
        for (int i = 0; i < 5; i++) {
            log.append(101L, "Event-" + i);
        }
        assertEquals(1, log.segments().size());
    }

    @Test
    void secondSegmentBaseOffset() {
        PartitionLog log = new PartitionLog(0, 5);
        for (int i = 0; i < 6; i++) {
            log.append(101L, "Event-" + i);
        }
        assertEquals(2, log.segments().size());
        assertEquals(5, log.segments().getLast().baseOffset());
    }

    @Test
    void readOutOfBound() {
        PartitionLog log = new PartitionLog(0, 5);
        for (int i = 0; i < 5; i++) {
            log.append(101L, "Event-" + i);
        }
        assertTrue(log.readFrom(100, 10).isEmpty());
    }

    @Test
    void invalidOrderId() {
        KeyPartitioner keyPartitioner = new KeyPartitioner();
        assertThrows(IllegalArgumentException.class, () -> keyPartitioner.partitionFor(0, 3));
    }
}
