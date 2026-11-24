
package dev.kafka.simulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class ConsumerGroupSimulator {

    private final String groupId;
    private final String topic;

    private final Map<Integer, ReplicatedPartition> partitions;

    private final Set<String> members = new TreeSet<>();

    private final Map<Integer, String> assignments = new HashMap<>();

    private final Map<Integer, Long> positions = new HashMap<>();

    private final Map<Integer, Long> committedOffsets = new HashMap<>();

    private long generation = 0;

    public ConsumerGroupSimulator(
            String groupId,
            String topic,
            Map<Integer, ReplicatedPartition> partitions
                                 ) {
        // - groupId и topic не должны быть blank.
        // - partitions не должен быть null или пустым.
        // - Partition IDs должны быть >= 0.
        // - Partition значения не должны быть null.
        // - сохранить защитную копию partitions.
        validateNotEmpty(groupId, "groupId");
        validateNotEmpty(topic, "topic");
        validateNotNull(partitions, "partitions");
        validatePartitions(partitions);
        this.groupId = groupId;
        this.topic = topic;
        this.partitions = Map.copyOf(partitions);

    }

    public long join(String consumerId) {
        // - проверить consumerId.
        validateNotEmpty(consumerId, "consumerId");
        // - запретить повторное присоединение
        //   того же consumerId.
        if (members.contains(consumerId)) {
            throw new IllegalArgumentException("Consumer with id " + consumerId + " is already added to the group");
        }
        // - добавить Consumer в members.
        members.add(consumerId);
        // - выполнить rebalance().
        rebalance();
        // - вернуть новую generation.
        return generation();
    }

    public void failConsumer(String consumerId) {
        // - consumerId должен существовать в members.
        if (!members.contains(consumerId)) {
            throw new IllegalArgumentException("Consumer with id " + consumerId + " is not member of group " + groupId);
        }
        // - удалить Consumer из members.
        members.remove(consumerId);
        // - выполнить rebalance().
        rebalance();
        // - committedOffsets НЕ очищать.
    }

    private void rebalance() {
        //
        // 1. Запомнить старые assignments
        //    и positions.
        //
        Map<Integer, Long> positionSnapshot = Map.copyOf(positions);
        Map<Integer, String> assignmentSnapshot = Map.copyOf(assignments);
        // 2. Увеличить generation.
        generation++;
        //
        // 3. Пересчитать assignments по правилам
        //    Round Robin.
        //
        List<Integer> partitionNumbers = partitions.keySet()
                                                   .stream()
                                                   .sorted()
                                                   .toList();

        List<String> sortedConsumers = new ArrayList<>(members);


        Map<Integer, String> newAssignments = new HashMap<>();
        Map<Integer, Long> newPositions = new HashMap<>();

        // 4. Для каждой Partition:
        //
        //    Если владелец НЕ изменился:
        //        сохранить прежнюю position.
        //
        //    Если владелец изменился:
        //        position = committedOffset,
        //        а если его нет — 0.
        //
        if (!sortedConsumers.isEmpty()) {
            for (int i = 0; i < partitionNumbers.size(); i++) {
                int partitionId = partitionNumbers.get(i);
                String consumer = sortedConsumers.get(i % sortedConsumers.size());
                newAssignments.put(partitionId, consumer);
                if (!consumer.equals(assignmentSnapshot.get(partitionId))) {
                    newPositions.put(partitionId, committedOffsets.getOrDefault(partitionId, 0L));
                } else {
                    newPositions.put(partitionId, positionSnapshot.get(partitionId));
                }
            }
            // 5. Если members пуст:
            //    assignments и positions пустые,
            //    committedOffsets сохраняются.
            assignments.clear();
            assignments.putAll(newAssignments);
            positions.clear();
            positions.putAll(newPositions);

        } else {
            positions.clear();
            assignments.clear();
        }
    }

    public List<SimRecord> poll(
            String consumerId,
            long expectedGeneration,
            int partitionId,
            int maxRecords) {
        // - maxRecords > 0.
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords cannot be <= 0");
        }
        // - взять текущую position.
        long curPos = position(consumerId, expectedGeneration, partitionId);

        // - вызвать partition.readVisible(
        //       position, maxRecords
        //   ).
        // - продвинуть position до:
        //       lastReturnedOffset + 1.
        // - если записей нет, position не меняется.
        // - committedOffset НЕ изменять.
        // - вернуть прочитанные записи.
        ReplicatedPartition partition = partitions.get(partitionId);
        List<SimRecord> records = partition.readVisible(curPos, maxRecords);
        if (!records.isEmpty()) {
            long lastReadOffset = records.getLast().offset();
            positions.put(partitionId, lastReadOffset + 1);
        }
        return records;
    }

    public void commit(
            String consumerId,
            long expectedGeneration,
            int partitionId,
            long nextOffset) {
        // - проверить generation и владельца.
        long position = position(consumerId, expectedGeneration, partitionId);
        // - nextOffset >= 0.
        // - nextOffset <= текущей position.
        if (nextOffset < 0 || nextOffset > position) {
            throw new IllegalArgumentException("nextOffset cannot be < 0 or > position");
        }
        // - запретить уменьшать ранее сохранённый
        //   committedOffset в нашей модели.
        // - сохранить nextOffset в committedOffsets.
        Long co = committedOffset(partitionId);
        if (co != null && nextOffset < co) {
            throw new IllegalArgumentException("nextOffset cannot be less than previous value");
        }
        committedOffsets.put(partitionId, nextOffset);
        //
        // ВНИМАНИЕ:
        // метод НЕ знает, завершилась ли
        // бизнес-обработка.
        // Корректный момент commit определяет
        // вызывающее приложение.
    }

    public long position(
            String consumerId,
            long expectedGeneration,
            int partitionId
                        ) {
        // - проверить generation и владельца.
        // - вернуть positions.get(partitionId).
        // - проверить generation.
        if (generation != expectedGeneration) {
            throw new IllegalStateException(
                    "Expected generation: " + expectedGeneration + ", actual: " + generation);
        }
        // - Consumer должен быть активным.
        if (!members.contains(consumerId)) {
            throw new IllegalArgumentException("No active consumer with id " + consumerId);
        }
        // - Consumer должен владеть Partition.
        String cons = assignments.get(partitionId);
        if (cons == null || !cons.equals(consumerId)) {
            throw new IllegalArgumentException("There is no consumer for partition or consumer is incorrect");
        }
        // - взять текущую position.
        return positions.get(partitionId);
    }

    public Long committedOffset(int partitionId) {
        // - проверить существование Partition.
        if (!partitions.containsKey(partitionId)) {
            throw new IllegalArgumentException("Partition with id " + partitionId + " doesn't exist");
        }
        // - вернуть сохранённый offset,
        //   либо null, если commit ещё не было.
        return committedOffsets.get(partitionId);
    }

    public Set<Integer> partitionsFor(String consumerId) {
        // - вернуть назначенные Consumer Partitions.
        // - неизвестный Consumer отклонить.
        // - вернуть защитную копию результата.
        if (!members.contains(consumerId)) {
            throw new IllegalArgumentException(
                    "Unknown consumer: " + consumerId
            );
        }
        Set<Integer> result = new HashSet<>();
        for (Map.Entry<Integer, String> entry : assignments.entrySet()) {
            if (entry.getValue().equals(consumerId)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    public String ownerOf(int partitionId) {
        // - вернуть текущего владельца.
        // - если нет активного владельца,
        //   допускается null.
        // - неизвестную Partition отклонить.

        if (!partitions.containsKey(partitionId)) {
            throw new IllegalArgumentException("partitionId " + partitionId + " doesn't exist");
        }
        return assignments.get(partitionId);
    }

    public long generation() {
        return generation;
    }

    private void validateNotNull(Object obj, String label) {
        if (obj == null) {
            throw new IllegalArgumentException(label + " cannot be null");
        }
    }

    private void validateNotEmpty(String str, String label) {
        validateNotNull(str, label);
        if (str.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " cannot be empty");
        }
    }

    private static void validatePartitions(Map<Integer, ReplicatedPartition> partitions) {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("partitions cannot be empty");
        }
        boolean hasInvalidPartition = partitions.keySet()
                                                .stream()
                                                .anyMatch(val -> val < 0);
        if (hasInvalidPartition) {
            throw new IllegalArgumentException("partition has invalid value < 0");
        }
    }
}
