package dev.kafka.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionVisibilityTest {

    @Test
    void shouldReadNonTransactionalRecordInBothModes() {
        TransactionalPartition partition = newPartition();

        SimRecord record =
                partition.appendNonTransactional(101, "OrderCreated");

        replicateAll(partition, 1);

        assertEquals(1, partition.highWatermark());
        assertEquals(1, partition.lastStableOffset());

        assertEquals(
                List.of(record),
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                    );

        assertEquals(
                List.of(record),
                partition.read(0, 10, IsolationLevel.READ_UNCOMMITTED)
                    );
    }

    @Test
    void shouldBlockCommittedReadAtOpenTransaction() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("T1");

        SimRecord transactional =
                partition.appendTransactional(
                        "T1", 101, "PaymentRequested"
                                             );

        SimRecord ordinary =
                partition.appendNonTransactional(
                        102, "InventoryChecked"
                                                );

        replicateAll(partition, 2);

        assertEquals(2, partition.highWatermark());
        assertEquals(0, partition.lastStableOffset());

        assertEquals(
                List.of(transactional, ordinary),
                partition.read(0, 10, IsolationLevel.READ_UNCOMMITTED)
                    );

        assertTrue(
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                         .isEmpty()
                  );
    }

    @Test
    void shouldWaitForCommitMarkerReplication() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("T1");

        SimRecord record = partition.appendTransactional(
                "T1", 101, "PaymentRequested"
                                                        );

        replicateAll(partition, 1);

        partition.commitTransaction("T1");

        assertEquals(2, partition.leaderLeo());
        assertEquals(1, partition.highWatermark());

        assertEquals(
                TransactionStatus.COMMITTING,
                partition.transactionStatus("T1")
                    );

        assertEquals(0, partition.lastStableOffset());

        assertTrue(
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                         .isEmpty()
                  );

        replicateAll(partition, 2);

        assertEquals(
                TransactionStatus.COMMITTED,
                partition.transactionStatus("T1")
                    );

        assertEquals(2, partition.lastStableOffset());

        assertEquals(
                List.of(record),
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                    );
    }

    @Test
    void shouldSkipAbortedTransactionalRecords() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("T1");

        SimRecord aborted = partition.appendTransactional(
                "T1", 101, "PaymentRequested"
                                                         );

        SimRecord ordinary =
                partition.appendNonTransactional(
                        102, "InventoryChecked"
                                                );

        replicateAll(partition, 2);

        partition.abortTransaction("T1");
        replicateAll(partition, 3);

        assertEquals(
                TransactionStatus.ABORTED,
                partition.transactionStatus("T1")
                    );

        assertEquals(
                List.of(ordinary),
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                    );

        assertEquals(
                List.of(aborted, ordinary),
                partition.read(0, 10, IsolationLevel.READ_UNCOMMITTED)
                    );
    }


    @Test
    void shouldWaitForEarliestOpenTransaction() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("T1");
        partition.beginTransaction("T2");

        SimRecord first = partition.appendTransactional(
                "T1", 101, "T1-event"
                                                       );

        SimRecord second = partition.appendTransactional(
                "T2", 102, "T2-event"
                                                        );

        partition.commitTransaction("T2");
        replicateAll(partition, 3);

        assertEquals(3, partition.highWatermark());
        assertEquals(0, partition.lastStableOffset());

        assertTrue(
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                         .isEmpty()
                  );

        partition.abortTransaction("T1");
        replicateAll(partition, 4);

        assertEquals(4, partition.lastStableOffset());

        assertEquals(
                List.of(second),
                partition.read(0, 10, IsolationLevel.READ_COMMITTED)
                    );

        assertEquals(
                List.of(first, second),
                partition.read(0, 10, IsolationLevel.READ_UNCOMMITTED)
                    );
    }

    @Test
    void shouldApplyReadLimitAfterFiltering() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("T1");

        partition.appendTransactional("T1", 101, "Aborted");

        SimRecord a =
                partition.appendNonTransactional(102, "A");

        SimRecord b =
                partition.appendNonTransactional(103, "B");

        partition.abortTransaction("T1");
        replicateAll(partition, 4);

        assertEquals(
                List.of(a),
                partition.read(0, 1, IsolationLevel.READ_COMMITTED)
                    );

        assertEquals(
                List.of(a, b),
                partition.read(0, 2, IsolationLevel.READ_COMMITTED)
                    );
    }

    @Test
    void beginTransactionNoBlocksLSO() {
        TransactionalPartition partition = newPartition();
        partition.beginTransaction("T1");

        SimRecord a = partition.appendNonTransactional(101, "A");
        replicateAll(partition, 1);

        assertEquals(
                List.of(a),
                partition.read(0, 1, IsolationLevel.READ_COMMITTED)
                    );

        assertEquals(
                List.of(a),
                partition.read(0, 1, IsolationLevel.READ_UNCOMMITTED)
                    );
    }

    @Test
    void repeatOpenActiveTransaction() {
        TransactionalPartition partition = newPartition();
        partition.beginTransaction("T1");
        assertThrows(IllegalArgumentException.class, () -> partition.beginTransaction("T1"));
    }

    @Test
    void notAllowedWritingToFinishedTransaction() {
        TransactionalPartition partition = newPartition();
        partition.beginTransaction("T1");
        partition.appendTransactional("T1", 101, "A");
        partition.commitTransaction("T1");

        assertThrows(IllegalStateException.class,
                     () -> partition.appendTransactional("T1", 101, "B"));
    }

    @Test
    void notAllowedToRepeatCallCommit() {
        TransactionalPartition partition = newPartition();
        partition.beginTransaction("T1");
        partition.appendTransactional("T1", 101, "A");
        partition.commitTransaction("T1");
        assertThrows(IllegalStateException.class, () -> partition.commitTransaction("T1"));
    }

    @Test
    void controlMarkerHasOffsetNoVisibleForConsumer() {
        TransactionalPartition partition = newPartition();
        partition.beginTransaction("T1");
        partition.appendTransactional("T1", 101, "A");
        partition.commitTransaction("T1");
        partition.appendNonTransactional(101L, "B");
        assertEquals(3, partition.leaderLeo());
        replicateAll(partition, 3);
        assertEquals(3, partition.highWatermark());
        List<SimRecord> records = partition.read(0, 4, IsolationLevel.READ_COMMITTED);
        assertEquals(2, records.size());
        assertEquals(
                List.of(0L, 2L),
                records.stream()
                       .map(SimRecord::offset)
                       .toList()
                    );
    }

    @Test
    void removalFromIsrForwardHW() {
        TransactionalPartition partition = newPartition();
        partition.beginTransaction("T1");
        partition.appendTransactional("T1", 101, "A");
        replicateAll(partition, 1);
        partition.commitTransaction("T1");
        partition.replicateTo("B2", 2);
        assertEquals(1, partition.highWatermark());
        partition.removeFromIsr("B3");
        assertEquals(2, partition.highWatermark());
        assertEquals(2, partition.lastStableOffset());
    }

    @Test
    void shouldUseEarliestUnstableTransactionOffset() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("z");
        partition.beginTransaction("a");

        partition.appendTransactional("z", 101, "T1");
        partition.appendNonTransactional(102, "Ordinary");
        partition.appendTransactional("a", 103, "T2");

        replicateAll(partition, 3);

        assertEquals(3, partition.highWatermark());
        assertEquals(0, partition.lastStableOffset());

        assertTrue(
                partition.read(
                        0, 10, IsolationLevel.READ_COMMITTED
                              ).isEmpty()
                  );
    }

    @Test
    void shouldRejectCommitAndAbortOfEmptyTransaction() {
        TransactionalPartition partition = newPartition();

        partition.beginTransaction("T1");

        assertThrows(
                IllegalStateException.class,
                () -> partition.commitTransaction("T1")
                    );

        assertThrows(
                IllegalStateException.class,
                () -> partition.abortTransaction("T1")
                    );

        assertEquals(0, partition.leaderLeo());
        assertEquals(
                TransactionStatus.OPEN,
                partition.transactionStatus("T1")
                    );
    }

    private TransactionalPartition newPartition() {
        return new TransactionalPartition(
                0,
                3,
                50,
                "B1",
                List.of("B1", "B2", "B3"),
                2
        );
    }

    private void replicateAll(
            TransactionalPartition partition,
            long targetLeo
                             ) {
        partition.replicateTo("B2", targetLeo);
        partition.replicateTo("B3", targetLeo);
    }
}
