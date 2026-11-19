package dev.kafka.simulator;

import java.util.List;

public class ReplicatedPartition {

    private final PartitionLog leaderLog;
    private final ReplicaState replicaState;

    public ReplicatedPartition(
            int partitionId,
            int segmentCapacity,
            int indexIntervalBytes,
            String leaderId,
            List<String> replicaIds,
            int minInSyncReplicas) {
        // Создать PartitionLog и ReplicaState.
        // Первоначально LEO каждой реплики = 0.
        // HW = 0.
        this.leaderLog = new PartitionLog(partitionId, segmentCapacity, indexIntervalBytes);
        this.replicaState = new ReplicaState(leaderId, replicaIds, minInSyncReplicas);
    }

    public SimRecord appendForAcksAll(long orderId, String eventType) {
        // Если ISR меньше minInSyncReplicas:
        //   отклонить запись.
        if (!replicaState.hasMinIsr()) {
            throw new IllegalStateException("ISR less than minInSyncReplicas value");
        }
        //
        // Добавить запись в leaderLog.
        //
        SimRecord record = leaderLog.append(orderId, eventType);

        // Сообщить ReplicaState о новом LEO:
        // leaderLog.nextOffset().
        //
        replicaState.onLeaderAppend(leaderLog.nextOffset());
        // Вернуть SimRecord.
        //
        // ВАЖНО:
        // Возвращённый SimRecord означает
        // "запись добавлена лидером",
        // но ещё НЕ "acks=all подтверждён".
        return record;
    }

    public void replicateTo(
            String followerId,
            long targetLeo
                           ) {
        // Делегировать в ReplicaState.
        replicaState.replicateTo(followerId, targetLeo);
    }

    public void removeFromIsr(String followerId) {
        // Делегировать в ReplicaState.
        replicaState.removeFromIsr(followerId);
    }

    public void addToIsr(String followerId) {
        // Делегировать в ReplicaState.
        replicaState.addToIsr(followerId);
    }

    public boolean isReadyForAcksAll(SimRecord record) {
        // true только если:
        // - record принадлежит этой Partition
        // - число ISR >= minInSyncReplicas
        // - HW > record.offset
        //
        // Это проверка ТЕКУЩЕЙ готовности,
        // а не сохранённый исторический
        // результат подтверждения Producer.
        if (record == null) {
            return false;
        }

        if (record.partition() != leaderLog.getPartitionId()) {
            return false;
        }

        if (!replicaState.hasMinIsr()
                || record.offset() < 0
                || record.offset() >= replicaState.highWatermark()) {
            return false;
        }

        List<SimRecord> found = leaderLog.readFrom(record.offset(), 1);

        return !found.isEmpty()
                && found.getFirst().equals(record);
    }

    public List<SimRecord> readVisible(long offset, int maxRecords) {
        // 1. Проверить offset >= 0, maxRecords > 0.
        // 2. Использовать существующий
        //    PartitionLog.readFrom().
        // 3. Вернуть не более maxRecords записей,
        //    у которых record.offset < HW.
        // 4. Не выдавать записи, которые физически
        //    есть у Leader, но ещё не достигли HW.
        //
        // На этапе C1 это READ_UNCOMMITTED.
        // LSO пока не учитываем.
        if (offset < 0 || maxRecords <= 0) {
            throw new IllegalArgumentException(
                    "invalid values for offset or maxRecords: " + offset + ", " + maxRecords);
        }
        List<SimRecord> records = leaderLog.readFrom(offset, maxRecords);
        return records.stream()
                      .filter(rec -> rec.offset() < replicaState.highWatermark())
                      .toList();
    }

    public long leaderLeo() {
        return replicaState.leaderLeo();
    }

    public long highWatermark() {
        return replicaState.highWatermark();
    }

    public long replicaLeo(String replicaId) {
        return replicaState.replicaLeo(replicaId);
    }

    public int isrSize() {
        return replicaState.isr().size();
    }
}
