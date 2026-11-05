import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MessagingTradeoffsLab {

    record EmailTask(int orderId) {}

    static void sendEmail(EmailTask task)
            throws InterruptedException {

        Thread.sleep(600);

        System.out.println(
            "Email sent for order " + task.orderId()
        );
    }

    public static void main(String[] args)
            throws InterruptedException {

        System.out.println("=== DIRECT CALL ===");

        long start = System.nanoTime();

        sendEmail(new EmailTask(101));

        System.out.printf(
            "Direct call: %.2f ms%n",
            (System.nanoTime() - start) / 1_000_000.0
        );

        System.out.println("\n=== QUEUE ===");

        BlockingQueue<EmailTask> queue =
            new ArrayBlockingQueue<>(2);

        System.out.println(queue.offer(new EmailTask(102)));
        System.out.println(queue.offer(new EmailTask(103)));
        System.out.println(queue.offer(new EmailTask(104)));

        System.out.println("Queue size: " + queue.size());

        Thread worker = new Thread(() -> {
            try {
                for (int i = 0; i < 2; i++) {
                    EmailTask task = queue.take();
                    sendEmail(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        worker.start();

        System.out.println("Main thread continues");

        worker.join();

        System.out.println("Worker completed");
    }
}
