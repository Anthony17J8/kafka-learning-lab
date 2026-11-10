import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LogCompactionLab {

    record LogRecord(
            long offset,
            String key,
            String value,
            long timestampMs
    ) {
        boolean isTombstone() {
            return value == null;
        }
    }

    static class CompactedLog {

        final List<LogRecord> records = new ArrayList<>();

        long nextOffset = 0;

        long append(
                String key,
                String value,
                long timestampMs
                   ) {
            // Key не должен быть null или пустым.
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key can't be null or empty");
            }
            //
            // Создать LogRecord с nextOffset.
            //
            LogRecord logRecord = new LogRecord(nextOffset, key, value, timestampMs);
            // Добавить запись в records.
            records.add(logRecord);
            //
            long assignedOffset = nextOffset;
            // Увеличить nextOffset.
            nextOffset++;
            // Вернуть присвоенный offset.
            return assignedOffset;
        }

        void compact(
                long cleanableBeforeOffset,
                long nowMs,
                long deleteRetentionMs
                    ) {
            // Построить Map<String, Long>:
            // key -> offset последней записи.
            // Последнюю запись определяем
            // по всему журналу, включая
            // неочищаемую активную часть.
            Map<String, Long> keyOffset = records.stream()
                                                 .collect(Collectors.toMap(LogRecord::key,
                                                                           LogRecord::offset,
                                                                           (_, o2) -> o2));

            // Рассматривать для удаления только
            // records с offset < cleanableBeforeOffset.
            //
            Iterator<LogRecord> itr = records.iterator();
            while (itr.hasNext()) {
                LogRecord record = itr.next();

                if (record.offset() >= cleanableBeforeOffset) {
                    continue;
                }

                boolean outdated =
                        record.offset() != keyOffset.get(record.key());

                boolean expiredTombstone =
                        record.isTombstone() && nowMs - record.timestampMs() > deleteRetentionMs;

                if (outdated || expiredTombstone) {
                    itr.remove();
                }
            }
        }

        Map<String, String> restoreState() {
            // Пройти записи в порядке offsets.
            //
            // Если value != null:
            //   сохранить key -> value.
            //
            // Если value == null:
            //   удалить key из состояния.
            //
            // Использовать TreeMap для
            // предсказуемого вывода.
            Map<String, String> store = new TreeMap<>();
            for (LogRecord record : records) {
                if (!record.isTombstone()) {
                    store.put(record.key(), record.value());
                } else {
                    store.remove(record.key);
                }
            }
            return store;
        }

        List<LogRecord> readFrom(long offset) {
            // Вернуть сохранившиеся записи,
            // у которых record.offset >= offset.
            //
            // Между offsets могут быть пропуски.
            //
            // Не использовать offset как
            // индекс массива.
            List<LogRecord> result = new ArrayList<>();
            for (LogRecord rec : records) {
                if (rec.offset() >= offset) {
                    result.add(rec);
                }
            }
            return result;
        }

        void printLog() {
            for (LogRecord record : records) {
                System.out.printf(
                        "offset=%d, key=%s, value=%s%n",
                        record.offset(),
                        record.key(),
                        record.value()
                                 );
            }

            System.out.println("Next offset: " + nextOffset);
        }
    }

    public static void main(String[] args) {

        CompactedLog log = new CompactedLog();

        log.append("order-101", "CREATED", 0);
        log.append("order-102", "CREATED", 1000);
        log.append("order-101", "RESERVED", 2000);
        log.append("order-103", "CREATED", 3000);
        log.append("order-102", "PAID", 4000);
        log.append("order-101", "COMPLETED", 5000);

        // Tombstone для order-103.
        log.append("order-103", null, 6000);

        log.append("order-104", "CREATED", 7000);
        log.append("order-104", "PAID", 8000);
        log.append("order-105", "CREATED", 9000);

        System.out.println("=== ORIGINAL LOG ===");
        log.printLog();

        // Offset 9 и далее считаем активной частью.
        // Tombstone пока недостаточно старый.
        log.compact(9, 12000, 10000);

        System.out.println("\n=== FIRST COMPACTION ===");
        log.printLog();
        System.out.println("State: " + log.restoreState());

        // Время прошло, Tombstone устарел.
        log.compact(9, 20000, 10000);

        System.out.println("\n=== TOMBSTONE EXPIRED ===");
        log.printLog();
        System.out.println("State: " + log.restoreState());

        System.out.println("\n=== READ FROM OFFSET 6 ===");

        System.out.println(
                log.readFrom(6).stream()
                   .map(LogRecord::offset)
                   .toList()
                          );

        log.append("order-103", "RECREATED", 21000);
        System.out.println("Append order-103 RECREATED");
        log.printLog();
        System.out.println("State: " + log.restoreState());

        CompactedLog log2 = new CompactedLog();

        log2.append("user-1", "A", 1000); // offset 0
        log2.append("user-1", "B", 2000); // offset 1
        log2.append("user-1", "C", 3000); // offset 2

        log2.compact(2, 10000, 10000);
        System.out.println("Experiment B: ");
        log2.printLog();
        log2.restoreState();

        // TEST 1: restoreState не должен изменять журнал
        CompactedLog test1 = new CompactedLog();

        test1.append("user-1", "Alice", 1000);
        test1.append("user-1", null, 2000);

        int before = test1.records.size();

        System.out.println(test1.restoreState());

        int after = test1.records.size();

        System.out.println("Before: " + before);
        System.out.println("After: " + after);


        // TEST 2: Tombstone в активной части
        CompactedLog test2 = new CompactedLog();

        test2.append("user-1", "Alice", 1000);
        test2.append("user-1", null, 2000);

        // Очищаем только offset 0.
        // Offset 1 находится в активной части.
        test2.compact(1, 20000, 10000);

        test2.printLog();
    }
}