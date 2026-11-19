package dev.kafka.simulator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaState {

    private final String leaderId;
    private final int minInSyncReplicas;

    private final Map<String, Long> replicaLeos = new HashMap<>();

    private final Set<String> isr = new HashSet<>();

    private long highWatermark = 0;

    public ReplicaState(String leaderId,
                        List<String> replicaIds,
                        int minInSyncReplicas) {
        // 1. Проверить входные аргументы.
        // 2. leaderId должен присутствовать
        //    в replicaIds.
        // 3. Все replicaIds должны быть уникальны.
        // 4. minInSyncReplicas должен находиться
        //    в диапазоне [1, replicaIds.size()].
        // 5. Инициализировать LEO всех реплик = 0.
        // 6. Первоначально все реплики входят в ISR.
        if (leaderId == null || leaderId.isEmpty()) {
            throw new IllegalArgumentException("leaderId cannot be null or empty");
        }
        if (replicaIds == null || replicaIds.isEmpty()) {
            throw new IllegalArgumentException("replicaIds cannot be empty");
        }
        isr.addAll(replicaIds);
        if (replicaIds.size() != isr.size()) {
            throw new IllegalStateException("replicaIds are not unique");
        }
        if (!isr.contains(leaderId)) {
            throw new IllegalStateException("ISR doesn't contain leader");
        }
        if (minInSyncReplicas < 1 || minInSyncReplicas > isr.size()) {
            throw new IllegalArgumentException("invalid value for minInSyncReplicas");
        }
        this.leaderId = leaderId;
        this.minInSyncReplicas = minInSyncReplicas;
        isr.forEach(rep -> replicaLeos.put(rep, 0L));
    }

    public void onLeaderAppend(long newLeaderLeo) {
        long currentLeo = leaderLeo();

        if (newLeaderLeo != currentLeo + 1) {
            throw new IllegalArgumentException(
                    "Expected LEO " + (currentLeo + 1)
                            + ", received " + newLeaderLeo
            );
        }

        replicaLeos.put(leaderId, newLeaderLeo);
        updateHighWatermark();
    }

    public void replicateTo(String followerId, long targetLeo) {
        // - replicaId должен существовать
        // - нельзя использовать leaderId
        // - targetLeo не может быть меньше
        //   текущего LEO этой реплики
        // - targetLeo не может быть больше
        //   LEO лидера
        // - обновить follower LEO
        // - пересчитать HW
        if (!replicaLeos.containsKey(followerId)) {
            throw new IllegalArgumentException("replicaId " + followerId + " doesn't exist");
        }
        if (followerId.equals(leaderId)) {
            throw new IllegalArgumentException("using leaderId for replication is not allowed");
        }
        if (targetLeo < replicaLeos.get(followerId)) {
            throw new IllegalArgumentException("current leo cannot be less than target leo");
        }
        if (targetLeo > leaderLeo()) {
            throw new IllegalArgumentException("target leo cannot be more than leader leo");
        }
        replicaLeos.put(followerId, targetLeo);
        updateHighWatermark();
    }

    public void removeFromIsr(String replicaId) {
        // - проверить существование реплики
        // - не позволять удалить Leader
        // - исключить Follower из ISR
        // - пересчитать HW
        if (!replicaLeos.containsKey(replicaId)) {
            throw new IllegalArgumentException(
                    "Unknown replica: " + replicaId
            );
        }

        if (replicaId.equals(leaderId)) {
            throw new IllegalArgumentException(
                    "Leader cannot be removed from ISR"
            );
        }

        if (!isr.contains(replicaId)) {
            throw new IllegalStateException(
                    "Replica is already outside ISR"
            );
        }
        isr.remove(replicaId);
        updateHighWatermark();
    }

    public void addToIsr(String replicaId) {
        // - проверить существование реплики
        // - разрешить возвращение Follower в ISR
        //   только если его LEO == LEO лидера
        // - пересчитать HW
        if (!replicaLeos.containsKey(replicaId)) {
            throw new IllegalArgumentException("LEO for replica " + replicaId + " doesn't exist");
        }
        if (isr.contains(replicaId)) {
            throw new IllegalArgumentException("replicaId " + replicaId + " is already added");
        }
        if (replicaLeos.get(replicaId).equals(leaderLeo())) {
            isr.add(replicaId);
            updateHighWatermark();
        } else {
            throw new IllegalStateException("Follower has gap and cannot be added to ISR");
        }
    }

    private void updateHighWatermark() {
        // - если isr.size() < minInSyncReplicas,
        //   не продвигать HW
        // - иначе candidate = minimum LEO among ISR
        // - новый HW = max(старый HW, candidate)
        // - HW не может быть больше LEO лидера
        if (isr.size() < minInSyncReplicas) {
            return;
        }
        long candidate = leaderLeo();
        for (String reps : isr) {
            candidate = Math.min(replicaLeos.get(reps), candidate);
        }
        highWatermark = Math.max(highWatermark, candidate);
        if (replicaLeos.get(leaderId) < highWatermark) {
            throw new IllegalStateException("leader lea cannot be less than HW");
        }
    }

    public long leaderLeo() {
        return replicaLeos.get(leaderId);
    }

    public long replicaLeo(String replicaId) {
        if (replicaId == null || replicaId.isEmpty()) {
            throw new IllegalArgumentException("replicaId cannot be null or empty");
        }
        if (!replicaLeos.containsKey(replicaId)) {
            throw new IllegalStateException("replicaId doesn't exist");
        }
        return replicaLeos.get(replicaId);
    }

    public long highWatermark() {
        return highWatermark;
    }

    public boolean hasMinIsr() {
        return isr.size() >= minInSyncReplicas;
    }

    public Set<String> isr() {
        return Set.copyOf(isr);
    }
}
