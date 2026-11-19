package dev.kafka.simulator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TransactionalPartition {

    private static final String COMMIT_MARKER =
            "__TX_COMMIT__";

    private static final String ABORT_MARKER =
            "__TX_ABORT__";

    private final ReplicatedPartition partition;

    private static final class TransactionState {
        final String transactionId;
        Long firstOffset;
        Long markerOffset;
        TransactionStatus status = TransactionStatus.OPEN;

        TransactionState(String transactionId) {
            this.transactionId = transactionId;
        }
    }

    private final Map<String, TransactionState> transactions = new HashMap<>();

    // Offset пользовательской записи -> ID транзакции.
    private final Map<Long, String> transactionByOffset = new HashMap<>();

    // Offsets служебных COMMIT/ABORT markers.
    private final Set<Long> controlOffsets = new HashSet<>();

    public TransactionalPartition(
            int partitionId,
            int segmentCapacity,
            int indexIntervalBytes,
            String leaderId,
            List<String> replicaIds,
            int minInSyncReplicas
                                 ) {
        this.partition = new ReplicatedPartition(
                partitionId,
                segmentCapacity,
                indexIntervalBytes,
                leaderId,
                replicaIds,
                minInSyncReplicas
        );
    }

    public void beginTransaction(String transactionId) {
        // - transactionId не null и не blank.
        // - нельзя повторно использовать тот же ID.
        // - создать TransactionState(OPEN).
        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException("transactionId cannot be null or blank");
        }
        if (transactions.containsKey(transactionId)) {
            throw new IllegalArgumentException("transactionId " + transactionId + " is already exist");
        }
        TransactionState txState = new TransactionState(transactionId);
        transactions.put(transactionId, txState);
    }

    public SimRecord appendNonTransactional(long orderId, String eventType) {
        // - eventType не должен быть служебным.
        // - делегировать appendForAcksAll().
        // - не добавлять offset в transactionByOffset.
        validateBusinessEventType(eventType);
        return partition.appendForAcksAll(orderId, eventType);
    }

    private void validateBusinessEventType(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException(
                    "eventType must not be blank"
            );
        }

        if (COMMIT_MARKER.equals(eventType)
                || ABORT_MARKER.equals(eventType)) {
            throw new IllegalArgumentException(
                    "Reserved eventType"
            );
        }
    }

    public SimRecord appendTransactional(String transactionId, long orderId, String eventType) {
        // - transactionId должен существовать.
        // - состояние должно быть OPEN.
        // - запретить служебный eventType.
        // - добавить запись через ReplicatedPartition.
        // - если firstOffset == null:
        //     сохранить offset первой записи.
        // - связать offset с transactionId.
        TransactionState txState = transactions.get(transactionId);
        if (txState == null) {
            throw new IllegalStateException("Transaction with id " + transactionId + " doesn't exist");
        }
        TransactionStatus txStatus = txState.status;
        if (txStatus != TransactionStatus.OPEN) {
            throw new IllegalStateException(
                    "Transaction with id " + transactionId + " is not opened, but has status " + txState.status);
        }
        validateBusinessEventType(eventType);
        SimRecord record = partition.appendForAcksAll(orderId, eventType);
        if (txState.firstOffset == null) {
            txState.firstOffset = record.offset();
        }
        transactionByOffset.put(record.offset(), transactionId);
        return record;
    }

    public void commitTransaction(String transactionId) {
        // - разрешено только для OPEN.
        // - транзакция должна содержать данные.
        // - append служебного COMMIT_MARKER
        //   через partition.appendForAcksAll().
        // - запомнить markerOffset.
        // - добавить offset в controlOffsets.
        // - перевести в COMMITTING.
        // - вызвать refreshTransactions().
        finishTransaction(transactionId, COMMIT_MARKER, TransactionStatus.COMMITTING);
    }

    public void abortTransaction(String transactionId) {
        // Аналогично COMMIT:
        // - только OPEN с данными.
        // - append ABORT_MARKER.
        // - статус ABORTING.
        // - refreshTransactions().
        finishTransaction(transactionId, ABORT_MARKER, TransactionStatus.ABORTING);
    }

    private void finishTransaction(String transactionId,
                                   String txMarker,
                                   TransactionStatus status) {
        if (!transactions.containsKey(transactionId)) {
            throw new IllegalStateException("Transaction " + transactionId + " doesn't exist");
        }
        TransactionState txState = transactions.get(transactionId);
        if (txState.status != TransactionStatus.OPEN) {
            throw new IllegalStateException("Transaction " + transactionId + " is not opened");
        }
        if (txState.firstOffset == null) {
            throw new IllegalStateException("Cannot finish transaction without records: " + transactionId);
        }
        SimRecord commitRec = partition.appendForAcksAll(0L, txMarker);
        controlOffsets.add(commitRec.offset());
        txState.status = status;
        txState.markerOffset = commitRec.offset();
        refreshTransactions();
    }

    private void refreshTransactions() {
        // Для каждой транзакции:
        //
        // Если COMMITTING и HW > markerOffset:
        //     статус = COMMITTED.
        //
        // Если ABORTING и HW > markerOffset:
        //     статус = ABORTED.
        //
        // Если HW <= markerOffset:
        //     не менять статус.
        for (TransactionState txState : transactions.values()) {
            if (txState.status == TransactionStatus.COMMITTING && highWatermark() > txState.markerOffset) {
                txState.status = TransactionStatus.COMMITTED;
            }
            if (txState.status == TransactionStatus.ABORTING && highWatermark() > txState.markerOffset) {
                txState.status = TransactionStatus.ABORTED;
            }
        }
    }

    public void replicateTo(
            String followerId,
            long targetLeo
                           ) {
        partition.replicateTo(followerId, targetLeo);
        refreshTransactions();
    }

    public void removeFromIsr(String followerId) {
        partition.removeFromIsr(followerId);
        refreshTransactions();
    }

    public void addToIsr(String followerId) {
        partition.addToIsr(followerId);
        refreshTransactions();
    }

    public long lastStableOffset() {
        // 1. Начальное значение = HW.
        // 2. Найти минимальный firstOffset среди
        //    транзакций OPEN/COMMITTING/ABORTING,
        //    для которых firstOffset != null.
        // 3. Вернуть минимум найденного и HW.
        long lso = partition.highWatermark();
        for (TransactionState txState : transactions.values()) {
            if (txState.firstOffset != null && isOpenTx(txState)) {
                lso = Math.min(txState.firstOffset, lso);
            }
        }
        return lso;
    }

    public List<SimRecord> read(long offset,
                                int maxRecords,
                                IsolationLevel isolationLevel) {
        // 1. Проверить аргументы.
        //
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be < 0");
        }
        if (maxRecords <= 0) {
            throw new IllegalStateException("maxRecords cannot be <= 0");
        }
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        // 2. Взять записи из partition.readVisible().
        //    Для учебной реализации допустимо
        //    запросить все доступные записи,
        //    а затем применить фильтры.
        //
        // 3. В обоих режимах пропустить
        //    controlOffsets.
        //
        List<SimRecord> bsnRecords = partition.readVisible(offset, Integer.MAX_VALUE)
                                              .stream()
                                              .filter(rec -> !controlOffsets.contains(rec.offset()))
                                              .toList();

        // 4. READ_UNCOMMITTED:
        //    возвращать все пользовательские
        //    записи ниже HW.
        //
        // 5. READ_COMMITTED:
        //    - только offset < LSO;
        //    - обычные записи оставить;
        //    - транзакционные записи оставить,
        //      только если статус COMMITTED;
        //    - ABORTED пропустить.
        //
        // 6. Применять maxRecords ПОСЛЕ
        //    фильтрации.
        //
        // 7. Не изменять физический журнал.
        if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
            return bsnRecords.stream()
                             .filter(rec -> rec.offset() < highWatermark())
                             .limit(maxRecords)
                             .toList();
        } else {
            long lso = lastStableOffset();
            return bsnRecords.stream()
                             .filter(rec -> rec.offset() < lso)
                             .filter(rec -> {
                                 String txId = transactionByOffset.get(rec.offset());
                                 if (txId == null) {
                                     return true;
                                 } else {
                                     TransactionState state = transactions.get(txId);
                                     return state.status == TransactionStatus.COMMITTED;
                                 }
                             })
                             .limit(maxRecords)
                             .toList();
        }
    }

    public TransactionStatus transactionStatus(String transactionId) {
        // транзакции. Неизвестный ID отклонить.
        TransactionState txState = transactions.get(transactionId);
        if (txState == null) {
            throw new IllegalStateException("Transaction with id " + transactionId + " doesn't exist");
        }
        return txState.status;
    }

    public long highWatermark() {
        return partition.highWatermark();
    }

    public long leaderLeo() {
        return partition.leaderLeo();
    }

    private boolean isOpenTx(TransactionState state) {
        return state.status == TransactionStatus.OPEN
                || state.status == TransactionStatus.COMMITTING
                || state.status == TransactionStatus.ABORTING;
    }
}
