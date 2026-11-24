package dev.kafka.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerGroupTest {

    @Test
    void shouldDistributePartitionsAcrossConsumers() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(
                                0, newPartition(0, 0),
                                1, newPartition(1, 0),
                                2, newPartition(2, 0),
                                3, newPartition(3, 0)
                              )
                );

        group.join("C1");
        group.join("C2");

        assertEquals(
                Set.of(0, 2),
                group.partitionsFor("C1")
                    );

        assertEquals(
                Set.of(1, 3),
                group.partitionsFor("C2")
                    );
    }

    @Test
    void shouldAdvancePositionWithoutCommitting() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3))
                );

        long generation = group.join("C1");

        List<SimRecord> records =
                group.poll("C1", generation, 0, 2);

        assertEquals(2, records.size());

        assertEquals(
                2,
                group.position("C1", generation, 0)
                    );

        assertNull(group.committedOffset(0));
    }

    @Test
    void shouldReplayFromCommittedOffsetAfterFailure() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3))
                );

        group.join("C1");
        group.join("C2");

        long generation = group.generation();

        List<SimRecord> firstBatch =
                group.poll("C1", generation, 0, 3);

        assertEquals(
                List.of(0L, 1L, 2L),
                firstBatch.stream()
                          .map(SimRecord::offset)
                          .toList()
                    );

        // Обработаны только записи 0 и 1.
        group.commit("C1", generation, 0, 2);

        group.failConsumer("C1");

        long newGeneration = group.generation();

        assertEquals("C2", group.ownerOf(0));

        assertEquals(
                2,
                group.position("C2", newGeneration, 0)
                    );

        List<SimRecord> replayed =
                group.poll("C2", newGeneration, 0, 10);

        assertEquals(
                List.of(2L),
                replayed.stream()
                        .map(SimRecord::offset)
                        .toList()
                    );
    }

    @Test
    void shouldRejectCommitFromStaleGeneration() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3))
                );

        long oldGeneration = group.join("C1");

        group.poll("C1", oldGeneration, 0, 2);

        group.join("C2");

        assertTrue(group.generation() > oldGeneration);

        assertThrows(
                IllegalStateException.class,
                () -> group.commit(
                        "C1",
                        oldGeneration,
                        0,
                        2
                                  )
                    );

        assertNull(group.committedOffset(0));
    }

    @Test
    void shouldMaintainIndependentGroupOffsets() {
        ReplicatedPartition partition = newPartition(0, 3);

        Map<Integer, ReplicatedPartition> logs =
                Map.of(0, partition);

        ConsumerGroupSimulator notifications =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        logs
                );

        ConsumerGroupSimulator analytics =
                new ConsumerGroupSimulator(
                        "analytics",
                        "orders.events",
                        logs
                );

        long nGeneration = notifications.join("N1");
        long aGeneration = analytics.join("A1");

        notifications.poll("N1", nGeneration, 0, 3);
        analytics.poll("A1", aGeneration, 0, 3);

        notifications.commit("N1", nGeneration, 0, 2);
        analytics.commit("A1", aGeneration, 0, 1);

        assertEquals(2L, notifications.committedOffset(0));
        assertEquals(1L, analytics.committedOffset(0));
    }

    @Test
    void shouldRespectHighWatermark() {
        ReplicatedPartition partition =
                newPartition(0, 0);

        // Запись пока существует только у Leader.
        partition.appendForAcksAll(101, "OrderCreated");

        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, partition)
                );

        long generation = group.join("C1");

        assertTrue(
                group.poll("C1", generation, 0, 10).isEmpty()
                  );

        assertEquals(
                0,
                group.position("C1", generation, 0)
                    );

        partition.replicateTo("B2", 1);
        partition.replicateTo("B3", 1);

        assertEquals(
                1,
                group.poll("C1", generation, 0, 10).size()
                    );
    }

    @Test
    void shouldRejectCommitBeyondPosition() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3))
                );

        group.join("C1");
        group.poll("C1", 1, 0, 2);
        assertEquals(2, group.position("C1", 1, 0));
        assertThrows(IllegalArgumentException.class, () -> group.commit("C1", 1, 0, 3));
        assertEquals(2, group.position("C1", 1, 0));
        assertNull(group.committedOffset(0));

    }

    @Test
    void shouldPreservePositionForUnchangedAssignment() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3),
                               1, newPartition(1, 3))
                );

        group.join("C1");
        group.poll("C1", 1, 0, 2);
        group.join("C2");
        assertEquals("C1", group.ownerOf(0));
        assertEquals("C2", group.ownerOf(1));
        assertEquals(2, group.position("C1", 2, 0));
    }

    @Test
    void shouldRejectPollFromNonOwner() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3),
                               1, newPartition(1, 3))
                );

        group.join("C1");
        group.join("C2");
        assertThrows(IllegalArgumentException.class, () -> group.poll("C1", 2, 1, 2));
    }

    @Test
    void shouldKeepConsumersIdleWhenPartitionsAreFewer() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3),
                               1, newPartition(1, 3)
                              )
                );

        group.join("C1");
        group.join("C2");
        group.join("C3");
        assertTrue(group.partitionsFor("C3").isEmpty());
    }

    @Test
    void shouldResumeFromZeroWithoutCommit() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3),
                               1, newPartition(1, 3)
                              )
                );

        group.join("C1");
        group.join("C2");
        group.poll("C1", 2, 0, 2);
        assertEquals(2, group.position("C1", 2, 0));
        group.failConsumer("C1");
        assertEquals("C2", group.ownerOf(0));
        assertEquals(0, group.position("C2", 3, 0));
    }

    @Test
    void shouldRejectCommittedOffsetRegression() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 3),
                               1, newPartition(1, 3)
                              )
                );

        group.join("C1");
        group.poll("C1", 1, 0, 2);
        group.commit("C1", 1, 0, 2);
        assertThrows(IllegalArgumentException.class, () -> group.commit("C1", 1, 0, 1));
        assertEquals(2L, group.committedOffset(0));
    }

    @Test
    void shouldUpdateCommittedOffsetMultipleTimes() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 5))
                );

        long generation = group.join("C1");

        group.poll("C1", generation, 0, 5);

        group.commit("C1", generation, 0, 2);
        assertEquals(2L, group.committedOffset(0));

        group.commit("C1", generation, 0, 5);
        assertEquals(5L, group.committedOffset(0));
    }

    @Test
    void shouldSupportNonContiguousPartitionIds() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(
                                2, newPartition(2, 3),
                                5, newPartition(5, 3)
                              )
                );

        group.join("C1");
        long generation = group.join("C2");

        assertEquals("C1", group.ownerOf(2));
        assertEquals("C2", group.ownerOf(5));

        assertEquals(0, group.position("C1", generation, 2));
        assertEquals(0, group.position("C2", generation, 5));

        List<SimRecord> records =
                group.poll("C2", generation, 5, 2);

        assertEquals(2, records.size());

        assertEquals(
                2,
                group.position("C2", generation, 5)
                    );
    }

    @Test
    void shouldReturnNullForUnassignedPartition() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 0))
                );

        group.join("C1");
        group.failConsumer("C1");

        assertNull(group.ownerOf(0));
    }

    @Test
    void shouldRejectUnknownConsumerInPartitionsFor() {
        ConsumerGroupSimulator group =
                new ConsumerGroupSimulator(
                        "notifications",
                        "orders.events",
                        Map.of(0, newPartition(0, 0))
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> group.partitionsFor("unknown")
                    );
    }

    private ReplicatedPartition newPartition(
            int partitionId,
            int recordCount) {
        ReplicatedPartition partition =
                new ReplicatedPartition(
                        partitionId,
                        3,
                        50,
                        "B1",
                        List.of("B1", "B2", "B3"),
                        2
                );

        for (int i = 0; i < recordCount; i++) {
            partition.appendForAcksAll(
                    101L + i,
                    "Event-" + i
                                      );
        }

        if (recordCount > 0) {
            partition.replicateTo("B2", recordCount);
            partition.replicateTo("B3", recordCount);
        }

        return partition;
    }
}
