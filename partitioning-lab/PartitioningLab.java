import java.util.ArrayList;
import java.util.List;

public class PartitioningLab {

    private static final int PARTITION_COUNT = 4;

    record Event(int orderId, String type) {}

    public static void main(String[] args) {

        List<Event> events = List.of(
            new Event(101, "OrderCreated"),
            new Event(102, "OrderCreated"),
            new Event(101, "OrderPaid"),
            new Event(103, "OrderCreated"),
            new Event(102, "OrderPaid"),
            new Event(104, "OrderCreated")
        );

        List<List<Event>> partitions = new ArrayList<>();

        for (int i = 0; i < PARTITION_COUNT; i++) {
            partitions.add(new ArrayList<>());
        }

        for (Event event : events) {

            int partition = Math.floorMod(
                event.orderId(),
                PARTITION_COUNT
            );

            List<Event> log = partitions.get(partition);

            int offset = log.size();

            log.add(event);

            System.out.printf(
                "orderId=%d, type=%s, partition=%d, offset=%d%n",
                event.orderId(),
                event.type(),
                partition,
                offset
            );
        }

        System.out.println("\n=== PARTITION LOGS ===");

        for (int i = 0; i < partitions.size(); i++) {
            System.out.println(
                "Partition " + i + ": " + partitions.get(i)
            );
        }
    }
}
