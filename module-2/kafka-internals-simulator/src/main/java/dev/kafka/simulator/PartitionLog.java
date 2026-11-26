package dev.kafka.simulator;

import java.util.ArrayList;
import java.util.List;

public class PartitionLog {

    private final int partitionId;
    private final int segmentCapacity;

    private final List<LogSegment> segments = new ArrayList<>();
    private final int indexIntervalBytes;

    private long nextOffset = 0;
    private long logStartOffset = 0;

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

    public SimRecord append(long orderId, String eventType) {
        return appendAt(orderId, eventType, System.currentTimeMillis());
    }

    public SimRecord appendAt(long orderId, String eventType, long timestampMs) {
        if (eventType == null || eventType.isEmpty()) {
            throw new IllegalArgumentException("evenType cannot be null or empty");
        }
        if (timestampMs < 0) {
            throw new IllegalArgumentException("timestampMs cannot be < 0");
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
        active.append(record, timestampMs);
        // Увеличить nextOffset.
        nextOffset++;
        // Вернуть созданную запись.
        return record;
    }

    public List<SimRecord> readFrom(long offset, int maxRecords) {
        return readFromWithStats(offset, maxRecords).records();
    }

    public int deleteExpiredSegments(long nowMs, long retentionMs) {
        // 1. Проверить nowMs >= 0.
        // 2. Проверить retentionMs >= 0.
        if (nowMs < 0) {
            throw new IllegalArgumentException("nowMs cannot be < 0");
        }
        if (retentionMs < 0) {
            throw new IllegalArgumentException("retentionMs cannot be < 0");
        }

        //
        // 3. Вычислить cutoff:
        //    nowMs - retentionMs.
        //
        long cutoff = nowMs - retentionMs;
        int idx = 0;
        int bound = segments.size() - 1;
        // 4. Пока существует более одного сегмента:
        //    - взять самый старый сегмент;
        //    - если maxTimestampMs > cutoff:
        //        прекратить удаление;
        //    - иначе удалить старый сегмент.
        //
        // 5. Обновить logStartOffset.
        //
        // 6. Вернуть количество удалённых сегментов.
        //
        // 7. Не изменять nextOffset.
        int deleted = 0;
        while (idx < bound) {
            LogSegment seg = segments.get(idx);
            if (cutoff >= seg.maxTimestampMs()) {
                deleted++;
            } else {
                break;
            }
            idx++;
        }

        segments.subList(0, deleted).clear();
        this.logStartOffset = segments.getFirst().baseOffset();
        return deleted;
    }

    public long totalSizeBytes() {
        // Сумма seg.sizeBytes() всех существующих сегментов.
        return segments.stream()
                       .mapToLong(LogSegment::sizeBytes)
                       .sum();
    }


    public int enforceRetentionBytes(long retentionBytes) {
        // 1. retentionBytes >= 0.
        //
        if (retentionBytes < 0) {
            throw new IllegalArgumentException("retentionBytes cannot be < 0");
        }
        // 2. Пока общий размер > retentionBytes
        //    и существует больше одного сегмента:
        //       удалить самый старый сегмент.
        //
        // 3. Обновить logStartOffset.
        //
        // 4. НЕ изменять nextOffset.
        //
        // 5. Вернуть количество удалённых сегментов.
        //
        // ВАЖНО:
        // если один активный сегмент превышает лимит,
        // оставить его и завершить очистку.
        int deleted = 0;
        long total = totalSizeBytes();
        int segSize = segments().size();
        while (total > retentionBytes && deleted < segSize - 1) {
            LogSegment segDeleted = segments.get(deleted);
            total -= segDeleted.sizeBytes();
            deleted++;
        }
        segments.subList(0, deleted).clear();
        this.logStartOffset = segments.getFirst().baseOffset();
        return deleted;
    }

    public ReadResult readFromWithStats(long offset, int maxRecords) {
        if (offset < logStartOffset) {
            throw new OffsetOutOfRangeException(
                    "Requested offset " + offset
                            + " is below logStartOffset "
                            + logStartOffset
            );
        }

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

    public long logStartOffset() {
        return logStartOffset;
    }

    public List<LogSegment> segments() {
        return List.copyOf(segments);
    }

    public int getPartitionId() {
        return partitionId;
    }
}
