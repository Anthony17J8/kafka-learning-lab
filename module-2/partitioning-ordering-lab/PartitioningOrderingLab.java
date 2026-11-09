import java.util.ArrayList;
import java.util.List;

public class PartitioningOrderingLab {

    static final int MOCK_BATCH_SIZE = 2;

    record Event(
            String eventId,
            Long orderId,
            int sequence
    ) {
    }

    record StoredRecord(
            int partition,
            long offset,
            Event event
    ) {
    }

    static class Topic {

        private final int partitionCount;

        private final List<List<StoredRecord>> logs = new ArrayList<>();

        private int stickyPartition = 0;
        private int stickyCount = 0;

        Topic(int partitionCount) {
            if (partitionCount <= 0) {
                throw new IllegalArgumentException(
                        "partitionCount must be positive"
                );
            }

            this.partitionCount = partitionCount;

            for (int i = 0; i < partitionCount; i++) {
                logs.add(new ArrayList<>());
            }
        }

        int keyedPartition(long orderId) {
            if (orderId <= 0) {
                throw new IllegalArgumentException("orderId must be more than 0");
            }

            // Использовать Long.hashCode(orderId)
            // и Math.floorMod.
            //
            // Вернуть Partition в диапазоне
            // [0, partitionCount).
            return Math.floorMod(Long.hashCode(orderId), partitionCount);
        }

        int nextNullKeyPartition() {

            // Смоделировать Sticky Partitioning.
            //
            // Две последовательные записи без ключа
            // направлять в одну Partition.
            //
            // После двух записей переходить
            // к следующей Partition по кругу.
            //
            // Учитывать только записи с null-key.
            if (stickyCount == MOCK_BATCH_SIZE) {
                stickyPartition = (stickyPartition + 1) % partitionCount;
                stickyCount = 0;
            }
            stickyCount++;
            return stickyPartition;
        }

        StoredRecord append(Event event) {
            // 1. Если orderId != null, выбрать
            //    Partition через keyedPartition().
            // 2. Иначе использовать
            //    nextNullKeyPartition().

            int partition = event.orderId() != null
                            ? keyedPartition(event.orderId())
                            : nextNullKeyPartition();
            // 3. Offset — текущий размер журнала
            //    выбранной Partition.
            //
            List<StoredRecord> log = logs.get(partition);
            // 4. Создать StoredRecord.
            //
            StoredRecord record = new StoredRecord(partition, log.size(), event);
            // 5. Добавить запись в logs.
            log.add(record);
            //
            // 6. Вернуть StoredRecord.
            return record;
        }

        List<Integer> sequencesForOrder(long orderId) {
            // Найти Partition заказа.
            // Прочитать её журнал.
            //
            int partition = keyedPartition(orderId);
            List<Integer> sequences = new ArrayList<>();
            // Вернуть значения sequence
            // только для указанного orderId
            // в порядке offsets.
            //
            // Например, для заказа 101:
            // [1, 2, 3]
            List<StoredRecord> records = logs.get(partition);
            for (StoredRecord rec : records) {
                if (rec.event().orderId() == null) {
                    continue;
                }
                if (rec.event().orderId() == orderId) {
                    sequences.add(rec.event().sequence());
                }
            }
            return sequences;
        }

        void printLogs() {
            for (int p = 0; p < partitionCount; p++) {
                System.out.println("Partition " + p);

                for (StoredRecord record : logs.get(p)) {
                    System.out.printf(
                            "  offset=%d, key=%s, event=%s%n",
                            record.offset(),
                            record.event().orderId(),
                            record.event().eventId()
                                     );
                }
            }
        }
    }

    static List<Event> events() {
        return List.of(
                new Event("OrderCreated-101", 101L, 1),
                new Event("OrderCreated-102", 102L, 1),
                new Event("InventoryReserved-101", 101L, 2),
                new Event("OrderCreated-103", 103L, 1),
                new Event("PaymentCompleted-101", 101L, 3),
                new Event("InventoryReserved-102", 102L, 2),

                new Event("Metric-A", null, 1),
                new Event("Metric-B", null, 2),
                new Event("Metric-C", null, 3),
                new Event("Metric-D", null, 4),
                new Event("Metric-E", null, 5),
                new Event("Metric-F", null, 6),
                new Event("Metric-G", null, 7)
                      );
    }

    static void runScenario(int partitionCount) {
        Topic topic = new Topic(partitionCount);

        for (Event event : events()) {
            topic.append(event);
        }

        System.out.println(
                "\n=== PARTITIONS: " + partitionCount + " ==="
                          );

        topic.printLogs();

        System.out.println(
                "Order 101 sequences: "
                        + topic.sequencesForOrder(101L)
                          );

        System.out.println(
                "Order 102 sequences: "
                        + topic.sequencesForOrder(102L)
                          );


    }

    public static void main(String[] args) {
        runScenario(3);
        runScenario(4);
    }
}