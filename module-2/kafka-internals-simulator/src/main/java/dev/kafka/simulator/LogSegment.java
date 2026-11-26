package dev.kafka.simulator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LogSegment {

    private static final int HEADER_BYTES = 24;
    private static final int DEFAULT_INDEX_INTERVAL_BYTES = 50;

    private record StoredRecord(
            SimRecord record,
            int bytePosition,
            int sizeBytes,
            long timestampMs) {
    }

    private final long baseOffset;
    private final int indexIntervalBytes;

    private final List<StoredRecord> storedRecords = new ArrayList<>();

    private final OffsetIndex offsetIndex = new OffsetIndex();

    private int bytesWritten = 0;
    private int lastIndexedPosition = 0;
    private long maxTimestampMs = -1;

    public LogSegment(long baseOffset) {
        this(baseOffset, DEFAULT_INDEX_INTERVAL_BYTES);
    }

    public LogSegment(long baseOffset, int indexIntervalBytes) {
        if (baseOffset < 0 || indexIntervalBytes <= 0) {
            throw new IllegalArgumentException();
        }

        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long maxTimestampMs() {
        return maxTimestampMs;
    }

    public int size() {
        return storedRecords.size();
    }

    public int sizeBytes() {
        return bytesWritten;
    }

    public List<IndexEntry> indexEntries() {
        return offsetIndex.entries();
    }

    // Совместимость с тестами этапа A.
    public List<SimRecord> records() {
        return storedRecords.stream()
                            .map(StoredRecord::record)
                            .toList();
    }

    private int simulatedSizeBytes(SimRecord record) {
        return HEADER_BYTES + record.eventType().getBytes(StandardCharsets.UTF_8).length;
    }

    void append(SimRecord record) {
        append(record, System.currentTimeMillis());
    }

    void append(SimRecord record, long timestampMs) {
        if (timestampMs < 0) {
            throw new IllegalArgumentException("timestampMs cannot be < 0");
        }

        validateRecord(record);

        // Определить текущую bytePosition:
        // она равна bytesWritten.
        int bytePosition = bytesWritten;

        // Вычислить sizeBytes.
        int sizeBytes = simulatedSizeBytes(record);

        // Добавить индексную запись, если:
        // - индекс пока пуст
        // ИЛИ
        // - от последней индексированной позиции
        //   прошло >= indexIntervalBytes.
        //
        // relativeOffset = record.offset - baseOffset.
        // Для безопасного преобразования:
        // Math.toIntExact(...)
        if (offsetIndex.isEmpty() || bytePosition - lastIndexedPosition >= indexIntervalBytes) {
            offsetIndex.append(Math.toIntExact(record.offset() - baseOffset), bytePosition);
            lastIndexedPosition = bytePosition;
        }

        // Добавить StoredRecord.
        StoredRecord storedRecord = new StoredRecord(record, bytePosition, sizeBytes, timestampMs);
        storedRecords.add(storedRecord);
        this.maxTimestampMs = Math.max(timestampMs, maxTimestampMs);

        // Увеличить bytesWritten на sizeBytes.
        // Желательно использовать Math.addExact().
        bytesWritten = Math.addExact(bytesWritten, sizeBytes);
    }

    private void validateRecord(SimRecord record) {
        // Проверить:
        // - record не null
        // - eventType не null и не пустой
        // - offset >= baseOffset
        // - offsets возрастают строго по порядку
        if (record == null) {
            throw new IllegalArgumentException("record cannot be null");
        }
        if (record.eventType() == null || record.eventType().isEmpty()) {
            throw new IllegalArgumentException("record.eventType cannot be empty");
        }
        if (record.offset() < baseOffset) {
            throw new IllegalArgumentException("record.offset must be more than base segment offset");
        }
        if (!storedRecords.isEmpty() && storedRecords.getLast().record().offset() >= record.offset()) {
            throw new IllegalArgumentException("Offsets must be in increasing order");
        }
    }

    public ReadResult readIndexed(long requestedOffset, int maxRecords) {
        // Проверить:
        // requestedOffset >= 0
        // maxRecords > 0
        if (requestedOffset < 0 || maxRecords <= 0) {
            throw new IllegalArgumentException("requestedOffset < 0 or maxRecords <= 0");
        }

        // Определить relative target:
        // requestedOffset - baseOffset.
        long relativeOffset = requestedOffset - baseOffset;

        // Найти floor() в offsetIndex.
        IndexEntry index = offsetIndex.floor(relativeOffset);

        // Если floor() вернул null:
        //   startBytePosition = 0.
        // Иначе:
        //   startBytePosition = entry.positionBytes().
        int startBytePosition = index == null ? 0 : index.positionBytes();

        // Найти StoredRecord, начинающийся
        // с полученной bytePosition.
        //
        // Можно использовать бинарный поиск
        // по storedRecords[].bytePosition.
        //
        // Не сканировать записи от начала сегмента
        // до startBytePosition.

        int leftIdx = 0;
        int rightIdx = storedRecords.size() - 1;
        int fromIdx = 0;
        while (leftIdx <= rightIdx) {
            int middle = leftIdx + ((rightIdx - leftIdx) / 2);
            if (storedRecords.get(middle).bytePosition() == startBytePosition) {
                fromIdx = middle;
                break;
            }
            if (storedRecords.get(middle).bytePosition() < startBytePosition) {
                leftIdx = middle + 1;
            } else {
                rightIdx = middle - 1;
            }
        }

        List<SimRecord> resultList = new ArrayList<>();
        int scanned = 0;
        // С этой записи последовательно
        // проверять offsets.
        //
        // Добавлять в результат только
        // record.offset >= requestedOffset.
        //
        // Остановиться после maxRecords
        // найденных записей.

        // Подсчитывать scannedRecords
        // для каждой реально просмотренной записи.
        for (int i = fromIdx; i < storedRecords.size() && resultList.size() < maxRecords; i++) {
            scanned++;
            StoredRecord storedRecord = storedRecords.get(i);
            if (storedRecord.record().offset() >= requestedOffset) {
                resultList.add(storedRecord.record());
            }
        }
        return new ReadResult(resultList, scanned);
    }

    public long lastOffset() {
        if (size() == 0) {
            return baseOffset - 1;
        }

        return storedRecords.getLast().record().offset();
    }
}
