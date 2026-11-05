import java.util.ArrayList;
import java.util.List;

public class SegmentedLogLab {

    static final int MAX_RECORDS_PER_SEGMENT = 3;

    record LogRecord(long offset, String value) {
    }

    static class Segment {
        final long baseOffset;
        final List<LogRecord> records = new ArrayList<>();

        Segment(long baseOffset) {
            this.baseOffset = baseOffset;
        }
    }

    static class PartitionLog {
        final List<Segment> segments = new ArrayList<>();
        long nextOffset = 0;

        void append(String value) {
            // 1. Если сегментов нет, создать первый.
            if (segments.isEmpty()) {
                Segment sgm = new Segment(nextOffset);
                segments.add(sgm);
            }
            // 2. Если текущий сегмент заполнен, создать новый.
            Segment current = segments.getLast();
            if (current.records.size() == MAX_RECORDS_PER_SEGMENT) {
                Segment sgm = new Segment(nextOffset);
                segments.add(sgm);
            }
            // 3. Добавить запись с nextOffset.
            segments.getLast().records.add(new LogRecord(nextOffset, value));

            // 4. Увеличить nextOffset.
            nextOffset++;
        }

        List<LogRecord> readFrom(long offset) {
            if (segments.isEmpty()) {
                return List.of();
            }

            // Если offset меньше первого доступного,
            // выбросить IllegalArgumentException.
            Segment first = segments.getFirst();
            if (first.baseOffset > offset) {
                throw new IllegalArgumentException(
                        "Requested offset " + offset
                                + " is before log start offset "
                                + first.baseOffset
                );
            }

            // Вернуть доступные записи с offset >= запрошенного.
            List<LogRecord> records = new ArrayList<>();
            for (Segment seg : segments) {
                if (seg.baseOffset + seg.records.size() <= offset) {
                    continue;
                }
                if (seg.baseOffset >= offset) {
                    records.addAll(seg.records);
                } else {
                    for (LogRecord rec : seg.records) {
                        if (rec.offset >= offset) {
                            records.add(rec);
                        }
                    }
                }
            }


            return records;
        }

        void deleteOldestClosedSegment() {
            // Удалить самый старый сегмент,
            // но не удалять единственный или активный сегмент.
            if (segments.size() > 1) {
                segments.removeFirst();
            }
        }

        void printSegments() {
            for (Segment segment : segments) {
                System.out.println(
                        "Segment baseOffset=" + segment.baseOffset
                                  );

                for (LogRecord record : segment.records) {
                    System.out.println(
                            "  " + record.offset()
                                    + " -> " + record.value()
                                      );
                }
            }
        }
    }

    public static void main(String[] args) {
        PartitionLog log = new PartitionLog();

        for (int i = 0; i < 8; i++) {
            log.append("Event-" + i);
        }

        System.out.println("=== INITIAL LOG ===");
        log.printSegments();

        System.out.println("=== READ FROM OFFSET 4 ===");
        System.out.println(log.readFrom(4));

        log.deleteOldestClosedSegment();

        System.out.println("=== AFTER RETENTION ===");
        log.printSegments();

        System.out.println("Next offset: " + log.nextOffset);

        try {
            log.readFrom(0);
            System.out.println("ERROR: Expected exception");
        } catch (IllegalArgumentException e) {
            System.out.println("Offset out of range rejected");
        }

        log.append("Event-8");
        log.append("Event-9");

        log.printSegments();
    }
}