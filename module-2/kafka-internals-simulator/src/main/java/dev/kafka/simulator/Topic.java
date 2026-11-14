package dev.kafka.simulator;

import java.util.ArrayList;
import java.util.List;

public class Topic {

    private final List<PartitionLog> partitions = new ArrayList<>();

    private final KeyPartitioner partitioner = new KeyPartitioner();

    public Topic(int partitionCount, int segmentCapacity) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException(
                    "partitionCount must be positive"
            );
        }

        for (int i = 0; i < partitionCount; i++) {
            partitions.add(
                    new PartitionLog(i, segmentCapacity)
                          );
        }
    }

    public SimRecord publish(
            long orderId,
            String eventType
                            ) {
        // 1. Выбрать Partition через partitioner.
        int partition = partitioner.partitionFor(orderId, partitionCount());
        // 2. Получить нужный PartitionLog.
        PartitionLog partitionLog = partitions.get(partition);
        // 3. Добавить запись.
        // 4. Вернуть SimRecord.
        return partitionLog.append(orderId, eventType);
    }

    public PartitionLog partition(int id) {
        if (partitions.size() - 1 < id) {
            throw new IllegalArgumentException("partition " + id + " doesn't exist");
        }
        return partitions.get(id);
    }

    public int partitionCount() {
        return partitions.size();
    }
}
