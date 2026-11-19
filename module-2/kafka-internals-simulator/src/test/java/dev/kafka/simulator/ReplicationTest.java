package dev.kafka.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationTest {

    private ReplicatedPartition newPartition() {
        return new ReplicatedPartition(
                0,
                3,
                50,
                "B1",
                List.of("B1", "B2", "B3"),
                2
        );
    }

    @Test
    void shouldInitializeReplicaState() {
        ReplicatedPartition partition = newPartition();

        assertEquals(0, partition.leaderLeo());
        assertEquals(0, partition.highWatermark());
        assertEquals(0, partition.replicaLeo("B2"));
        assertEquals(0, partition.replicaLeo("B3"));
        assertEquals(3, partition.isrSize());
    }

    @Test
    void shouldNotExposeUnreplicatedRecord() {
        ReplicatedPartition partition = newPartition();

        SimRecord record =
                partition.appendForAcksAll(101, "OrderCreated");

        assertEquals(0, record.offset());
        assertEquals(1, partition.leaderLeo());
        assertEquals(0, partition.highWatermark());

        assertFalse(partition.isReadyForAcksAll(record));
        assertTrue(partition.readVisible(0, 10).isEmpty());
    }

    @Test
    void shouldAdvanceHwAfterAllIsrReplicate() {
        ReplicatedPartition partition = newPartition();

        SimRecord record =
                partition.appendForAcksAll(101, "OrderCreated");

        partition.replicateTo("B2", 1);

        assertEquals(0, partition.highWatermark());
        assertFalse(partition.isReadyForAcksAll(record));

        partition.replicateTo("B3", 1);

        assertEquals(1, partition.highWatermark());
        assertTrue(partition.isReadyForAcksAll(record));

        assertEquals(
                List.of(record),
                partition.readVisible(0, 10)
                    );
    }

    @Test
    void shouldAdvanceHwWhenLaggingReplicaLeavesIsr() {
        ReplicatedPartition partition = newPartition();

        SimRecord record =
                partition.appendForAcksAll(101, "OrderCreated");

        partition.replicateTo("B2", 1);

        // B3 ещё не получила запись.
        assertEquals(0, partition.highWatermark());

        // ISR теперь B1, B2.
        partition.removeFromIsr("B3");

        assertEquals(2, partition.isrSize());
        assertEquals(1, partition.highWatermark());
        assertTrue(partition.isReadyForAcksAll(record));
    }

    @Test
    void shouldRejectWriteWhenIsrBelowMinimum() {
        ReplicatedPartition partition = newPartition();

        partition.removeFromIsr("B2");
        partition.removeFromIsr("B3");

        assertThrows(
                IllegalStateException.class,
                () -> partition.appendForAcksAll(
                        101, "OrderCreated"
                                                )
                    );

        assertEquals(0, partition.leaderLeo());
    }

    @Test
    void shouldRejectFollowerLeoAheadOfLeader() {
        ReplicatedPartition partition = newPartition();

        partition.appendForAcksAll(101, "OrderCreated");

        assertThrows(
                IllegalArgumentException.class,
                () -> partition.replicateTo("B2", 2)
                    );

        assertEquals(0, partition.replicaLeo("B2"));
    }

    @Test
    void shouldRejectFollowerRegression() {
        ReplicatedPartition partition = newPartition();

        partition.appendForAcksAll(101, "OrderCreated");
        partition.appendForAcksAll(101, "OrderPaid");

        partition.replicateTo("B2", 2);

        assertThrows(
                IllegalArgumentException.class,
                () -> partition.replicateTo("B2", 1));

        assertEquals(2, partition.replicaLeo("B2"));
    }

    @Test
    void shouldRejectRejoinFollowerWithGap() {
        ReplicatedPartition partition = newPartition();

        partition.appendForAcksAll(101, "OrderCreated");
        partition.appendForAcksAll(101, "OrderPaid");

        partition.replicateTo("B2", 2);
        partition.replicateTo("B3", 1);

        partition.removeFromIsr("B3");

        assertEquals(2, partition.highWatermark());
        assertThrows(IllegalStateException.class, () ->partition.addToIsr("B3"));

        partition.replicateTo("B3", 2);
        partition.addToIsr("B3");

        assertEquals(3, partition.isrSize());
        assertEquals(2, partition.highWatermark());
    }

    @Test
    void shouldBeVisibleOnlyReplicatedEntries() {
        ReplicatedPartition partition = newPartition();

        SimRecord first =
                partition.appendForAcksAll(101, "OrderCreated");

        partition.replicateTo("B2", 1);
        partition.replicateTo("B3", 1);

        partition.appendForAcksAll(101, "OrderPaid");

        assertEquals(1, partition.highWatermark());

        assertEquals(
                List.of(first),
                partition.readVisible(0, 10)
                    );

        assertTrue(
                partition.readVisible(1, 10).isEmpty()
                  );
    }

    @Test
    void shouldBeStableHWAfterISRRemoval() {
        ReplicatedPartition partition = newPartition();

        partition.appendForAcksAll(101, "OrderCreated");
        partition.appendForAcksAll(101, "OrderPaid");

        partition.replicateTo("B2", 2);
        partition.replicateTo("B3", 2);
        assertEquals(2, partition.highWatermark());

        partition.removeFromIsr("B2");
        partition.removeFromIsr("B3");
        assertEquals(2, partition.highWatermark());
    }

    @Test
    void shouldRejectInvalidLeaderLeo() {
        ReplicaState state = new ReplicaState(
                "B1",
                List.of("B1", "B2", "B3"),
                2
        );

        state.onLeaderAppend(1);

        assertThrows(
                IllegalArgumentException.class,
                () -> state.onLeaderAppend(5)
                    );

        assertEquals(1, state.leaderLeo());
    }

    @Test
    void shouldNotAcknowledgeUnknownRecord() {
        ReplicatedPartition partition = newPartition();

        partition.appendForAcksAll(101, "OrderCreated");

        partition.replicateTo("B2", 1);
        partition.replicateTo("B3", 1);

        SimRecord fake = new SimRecord(
                0,
                0,
                999,
                "FakePaymentCompleted"
        );

        assertFalse(partition.isReadyForAcksAll(fake));
    }
}

