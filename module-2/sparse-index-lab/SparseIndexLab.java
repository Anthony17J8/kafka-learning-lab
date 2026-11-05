import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparseIndexLab {

    static final int INDEX_INTERVAL_RECORDS = 4;

    record LogRecord(
            long offset,
            int bytePosition,
            int sizeBytes,
            String value
    ) {
    }

    record IndexEntry(
            int relativeOffset,
            int bytePosition
    ) {
    }

    record ReadResult(
            LogRecord record,
            IndexEntry anchor,
            int scannedRecords
    ) {
    }

    static class Segment {

        final long baseOffset;

        // Учебная имитация файла:
        // ключ — начальная позиция записи в байтах.
        final Map<Integer, LogRecord> file = new HashMap<>();

        final List<IndexEntry> index = new ArrayList<>();

        int nextBytePosition = 0;
        int recordCount = 0;

        Segment(long baseOffset) {
            this.baseOffset = baseOffset;
        }

        void append(String value, int sizeBytes) {
            // Проверить, что sizeBytes > 0.
            if (sizeBytes <= 0) {
                throw new IllegalArgumentException(
                        "Record size must be positive: " + sizeBytes
                );
            }

            // Определить absolute offset:
            // baseOffset + recordCount.
            long absoluteOffset = baseOffset + recordCount;

            // Создать LogRecord с текущей
            // bytePosition = nextBytePosition.
            LogRecord logRecord = new LogRecord(absoluteOffset, nextBytePosition, sizeBytes, value);

            // Сохранить запись в file.
            file.put(logRecord.bytePosition, logRecord);

            // Если recordCount % 3 == 0,
            // добавить новую IndexEntry.
            // Offset внутри IndexEntry — относительный.
            if (recordCount % INDEX_INTERVAL_RECORDS == 0) {
                index.add(new IndexEntry((int) (logRecord.offset - baseOffset), logRecord.bytePosition));
            }

            // Увеличить nextBytePosition на sizeBytes.
            this.nextBytePosition = nextBytePosition + sizeBytes;
            // Увеличить recordCount.
            this.recordCount++;
        }

        IndexEntry floorIndex(long targetOffset) {
            // Бинарный поиск.
            //
            // Найти индексную запись с наибольшим
            // absolute offset <= targetOffset.
            //
            // Использовать:
            // absoluteOffset =
            //     baseOffset + entry.relativeOffset()
            //
            // Нельзя просто искать точное совпадение.
            if (index.isEmpty()) {
                return null;
            }
            int leftIdx = 0;
            int rightIdx = index.size() - 1;
            int result = -1;
            while (leftIdx <= rightIdx) {
                int middle = leftIdx + ((rightIdx - leftIdx) / 2);
                IndexEntry candidate = index.get(middle);
                long off = candidate.relativeOffset + baseOffset;
                if (off <= targetOffset) {
                    result = middle;
                    leftIdx = middle + 1;
                } else {
                    rightIdx = middle - 1;
                }
            }

            if (result == -1) {
                throw new IllegalStateException("Offset %d not found".formatted(targetOffset));
            }
            return index.get(result);
        }

        ReadResult read(long targetOffset) {
            // Проверить допустимый диапазон offsets.
            if (targetOffset < baseOffset || targetOffset >= baseOffset + recordCount) {
                throw new IllegalArgumentException("Offset %d rejected".formatted(targetOffset));
            }

            // Получить опорную позицию floorIndex().
            IndexEntry anchor = floorIndex(targetOffset);

            // Начиная с anchor.bytePosition(),
            // последовательно считывать LogRecord из file.
            ReadResult result = null;
            LogRecord logRecord = file.get(anchor.bytePosition);
            int scanned = 0;
            while (logRecord != null) {
                scanned++;
                if (logRecord.offset() == targetOffset) {
                    result = new ReadResult(logRecord, anchor, scanned);
                    break;
                }
                // Следующая позиция:
                // currentPosition + record.sizeBytes()
                logRecord = file.get(logRecord.bytePosition + logRecord.sizeBytes);
            }
            // Когда offset совпадёт с targetOffset,
            // вернуть ReadResult.

            if (result == null) {
                throw new IllegalStateException("Record not found at offset " + targetOffset);
            }
            return result;
        }

        void printIndex() {
            for (IndexEntry entry : index) {
                System.out.printf(
                        "relativeOffset=%d, bytePosition=%d%n",
                        entry.relativeOffset(),
                        entry.bytePosition()
                                 );
            }
        }
    }

    public static void main(String[] args) {

        Segment segment = new Segment(100);

        int[] sizes = {
                16, 24, 12, 28, 20, 16, 32, 12
        };

        for (int i = 0; i < sizes.length; i++) {
            segment.append("Event-" + i, sizes[i]);
        }

        System.out.println("=== INDEX ===");
        segment.printIndex();

        System.out.println("=== READ ===");

        for (long offset : new long[]{100, 103, 104, 105, 107}) {
            ReadResult result = segment.read(offset);

            System.out.printf(
                    "offset=%d, position=%d, "
                            + "anchor=%d, scanned=%d%n",
                    result.record().offset(),
                    result.record().bytePosition(),
                    segment.baseOffset
                            + result.anchor().relativeOffset(),
                    result.scannedRecords()
                             );
        }

        System.out.println("=== INVALID OFFSET ===");

        try {
            segment.read(99);
            System.out.println("ERROR: Expected exception");
        } catch (IllegalArgumentException e) {
            System.out.println("Offset 99 rejected");
        }

        try {
            segment.read(108);
            System.out.println("ERROR: Expected exception");
        } catch (IllegalArgumentException e) {
            System.out.println("Offset 108 rejected");
        }
    }
}