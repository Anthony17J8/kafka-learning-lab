import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConsumerOffsetLab {

    record LogRecord(int offset, String eventId) {
    }

    static class OffsetStore {
        int committedOffset = 0;
    }

    static class SimConsumer {

        final List<LogRecord> log;
        final OffsetStore store;

        // Имитация внешней БД.
        // key = offset
        // value = сколько раз выполнен бизнес-эффект.
        final Map<Integer, Integer> businessEffects;

        int position;

        SimConsumer(
                List<LogRecord> log,
                OffsetStore store,
                Map<Integer, Integer> businessEffects
                   ) {
            this.log = log;
            this.store = store;
            this.businessEffects = businessEffects;

            // После перезапуска восстанавливаем
            // позицию из committed offset.
            this.position = store.committedOffset;
        }

        List<LogRecord> poll(int maxRecords) {
            // Проверить, что maxRecords > 0.
            if (maxRecords <= 0) {
                throw new IllegalArgumentException("maxRecords must be > 0");
            }

            // Вернуть не более maxRecords,
            // начиная с position.
            int idx = 0;
            List<LogRecord> result = new ArrayList<>();
            while (idx < Math.min(maxRecords, log.size() - position)) {
                result.add(log.get(idx + position));
                idx++;
            }
            // Продвинуть position до позиции,
            // следующей за последней выданной записью.
            //
            // Важно:
            // poll() НЕ выполняет бизнес-обработку
            // и НЕ обновляет committedOffset.
            this.position += idx;
            return result;
        }

        void process(LogRecord record) {
            businessEffects.merge(
                    record.offset(),
                    1,
                    Integer::sum);
        }

        void commit(int nextOffset) {
            // Сохранить nextOffset в OffsetStore.
            //
            // Для нашей модели:
            // 1. nextOffset не может быть отрицательным.
            // 2. nextOffset не может превышать position.
            // 3. Нельзя уменьшать committedOffset.
            //
            // В реальной Kafka API допускает
            // более гибкое управление offsets.
            if (nextOffset < 0 || nextOffset > position || store.committedOffset > nextOffset) {
                throw new IllegalArgumentException("Invalid value for nextOffset");
            }
            store.committedOffset = nextOffset;
        }

        void simulateAutoCommit() {
            // Сохранить текущую position
            // в OffsetStore независимо от того,
            // завершились ли бизнес-операции.
            //
            // Это имитация наступившего момента
            // автоматического Commit.
            store.committedOffset = this.position;
        }
    }

    static List<LogRecord> createLog() {
        List<LogRecord> log = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            log.add(new LogRecord(i, "evt-" + i));
        }

        return log;
    }

    static List<Integer> offsets(List<LogRecord> records) {
        return records.stream()
                      .map(LogRecord::offset)
                      .toList();
    }

    public static void main(String[] args) {
        List<LogRecord> log = createLog();

        // СЦЕНАРИЙ A: повторная обработка
        System.out.println("=== A: DUPLICATE PROCESSING ===");

        OffsetStore storeA = new OffsetStore();
        Map<Integer, Integer> dbA = new TreeMap<>();

        SimConsumer a = new SimConsumer(log, storeA, dbA);
        List<LogRecord> batchA = a.poll(3);

        a.process(batchA.get(0));
        a.process(batchA.get(1));
        a.commit(2);

        // Бизнес-эффект выполнен,
        // но offset 3 ещё не сохранён.
        a.process(batchA.get(2));

        System.out.println(
                "Before crash: position=" + a.position
                        + ", committed=" + storeA.committedOffset
                          );

        // Имитируем аварийный перезапуск.
        SimConsumer a2 = new SimConsumer(log, storeA, dbA);
        List<LogRecord> replay = a2.poll(3);

        System.out.println(
                "After restart: " + offsets(replay)
                          );

        // Повторная бизнес-обработка offset 2.
        a2.process(replay.get(0));

        System.out.println(
                "Offset 2 business effects: "
                        + dbA.getOrDefault(2, 0)
                          );


        // СЦЕНАРИЙ B: пропуск обработки
        System.out.println("\n=== B: SKIPPED PROCESSING ===");

        OffsetStore storeB = new OffsetStore();
        Map<Integer, Integer> dbB = new TreeMap<>();

        SimConsumer b = new SimConsumer(log, storeB, dbB);
        List<LogRecord> batchB = b.poll(3);

        // Auto Commit произошёл до завершения
        // всех бизнес-операций.
        b.simulateAutoCommit();

        b.process(batchB.get(0));

        System.out.println(
                "Before crash: position=" + b.position
                        + ", committed=" + storeB.committedOffset
                          );

        SimConsumer b2 = new SimConsumer(log, storeB, dbB);

        System.out.println(
                "After restart: " + offsets(b2.poll(3))
                          );

        System.out.println(
                "Offset 1 was processed: "
                        + dbB.containsKey(1)
                          );


        // СЦЕНАРИЙ C: успешная обработка + Commit
        System.out.println("\n=== C: PROCESS THEN COMMIT ===");

        OffsetStore storeC = new OffsetStore();
        Map<Integer, Integer> dbC = new TreeMap<>();

        SimConsumer c = new SimConsumer(log, storeC, dbC);
        List<LogRecord> batchC = c.poll(3);

        for (LogRecord record : batchC) {
            c.process(record);
        }

        c.commit(2);

        System.out.println(
                "Committed: " + storeC.committedOffset
                          );

        SimConsumer c2 = new SimConsumer(log, storeC, dbC);

        System.out.println(
                "After restart: " + offsets(c2.poll(3))
                          );
    }
}