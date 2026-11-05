import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MessagingModelsLab {

    static void processTask(Queue<String> queue, String worker) {
        String event = queue.poll();

        if (event != null) {
            System.out.println(worker + " -> " + event);
        }
    }

    static void readLog(
            List<String> log,
            String consumer,
            int startOffset,
            int count
    ) {
        int end = Math.min(startOffset + count, log.size());

        for (int offset = startOffset; offset < end; offset++) {
            System.out.printf(
                "%s: offset=%d, event=%s%n",
                consumer, offset, log.get(offset)
            );
        }
    }

    public static void main(String[] args) {
        List<String> events = List.of(
            "OrderCreated(101)",
            "OrderPaid(101)",
            "OrderCreated(102)",
            "OrderCancelled(102)"
        );

        System.out.println("=== WORK QUEUE ===");

        Queue<String> queue = new ArrayDeque<>(events);

        processTask(queue, "worker-1");
        processTask(queue, "worker-2");
        processTask(queue, "worker-1");
        processTask(queue, "worker-2");

        System.out.println("Queue size: " + queue.size());

        System.out.println("\n=== EVENT LOG ===");

        readLog(events, "notifications", 0, 2);
        readLog(events, "analytics", 0, 4);

        System.out.println("\n=== REPLAY ===");

        readLog(events, "notifications", 0, 2);

        System.out.println("Log size: " + events.size());
    }
}
