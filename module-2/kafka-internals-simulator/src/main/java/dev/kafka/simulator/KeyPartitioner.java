package dev.kafka.simulator;

public class KeyPartitioner {

    public int partitionFor(
            long orderId,
            int partitionCount) {
        // 1. orderId должен быть положительным.
        if (orderId <= 0) {
            throw new IllegalArgumentException("orderId must be positive");
        }
        // 2. partitionCount должен быть > 0.
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        // 3. Вернуть номер Partition.
        //
        // Использовать Long.hashCode()
        // и Math.floorMod().
        return Math.floorMod(Long.hashCode(orderId), partitionCount);
    }
}
