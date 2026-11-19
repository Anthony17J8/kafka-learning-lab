package dev.kafka.simulator;

import java.util.ArrayList;
import java.util.List;

public class PartitionLog {

    private final int partitionId;
    private final int segmentCapacity;

    private final List<LogSegment> segments = new ArrayList<>();
    private final int indexIntervalBytes;

    private long nextOffset = 0;

    public PartitionLog(
            int partitionId,
            int segmentCapacity) {
        this(partitionId, segmentCapacity, 50);
    }

    public PartitionLog(
            int partitionId,
            int segmentCapacity,
            int indexIntervalBytes
                       ) {
        if (partitionId < 0
                || segmentCapacity <= 0
                || indexIntervalBytes <= 0) {
            throw new IllegalArgumentException();
        }

        this.partitionId = partitionId;
        this.segmentCapacity = segmentCapacity;
        this.indexIntervalBytes = indexIntervalBytes;

        segments.add(new LogSegment(0, indexIntervalBytes));
    }

    public SimRecord append(
            long orderId,
            String eventType
                           ) {
        if (eventType == null || eventType.isEmpty()) {
            throw new IllegalArgumentException("evenType cannot be null or empty");
        }

        // Получить активный сегмент.
        LogSegment active = segments.getLast();
        // Если активный сегмент заполнен,
        // создать новый с baseOffset = nextOffset.
        if (active.size() == segmentCapacity) {
            active = new LogSegment(nextOffset, indexIntervalBytes);
            segments.add(active);
        }

        // Создать SimRecord с:
        // partition = partitionId
        // offset = nextOffset
        // orderId = orderId
        // eventType = eventType
        SimRecord record = new SimRecord(partitionId, nextOffset, orderId, eventType);
        // Добавить запись в активный сегмент.
        active.append(record);
        // Увеличить nextOffset.
        nextOffset++;
        // Вернуть созданную запись.

        return record;
    }

    public List<SimRecord> readFrom(long offset, int maxRecords) {
        return readFromWithStats(offset, maxRecords).records();
    }

    public ReadResult readFromWithStats(long offset, int maxRecords) {
        // 1. Проверить аргументы.
        if (offset < 0 || maxRecords <= 0) {
            throw new IllegalArgumentException("offset < 0 or maxRecords <= 0");
        }

        //
        // 2. Найти подходящий сегмент.
        // На этом этапе допускается пройти
        // по списку сегментов.
        //
        // 3. Вызвать segment.readIndexed().
        //
        // 4. Если нужных записей недостаточно,
        // продолжить чтение в следующих сегментах.
        //
        // 5. Суммировать scannedRecords.
        //
        // 6. Вернуть не более maxRecords.
        //
        // Важно: не вызывать readIndexed()
        // с maxRecords = 0.

        List<SimRecord> records = new ArrayList<>();
        int scanned = 0;
        for (LogSegment seg : segments) {
            if (seg.lastOffset() < offset) {
                continue;
            }
            ReadResult readResult = seg.readIndexed(offset, maxRecords);
            records.addAll(readResult.records());
            scanned += readResult.scannedRecords();
            maxRecords -= readResult.records().size();

            if (maxRecords == 0) {
                break;
            }
        }
        return new ReadResult(records, scanned);
    }

    public long nextOffset() {
        return nextOffset;
    }

    public List<LogSegment> segments() {
        return List.copyOf(segments);
    }

    public int getPartitionId() {
        return partitionId;
    }
}
