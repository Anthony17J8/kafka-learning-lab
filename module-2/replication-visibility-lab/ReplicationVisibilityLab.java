import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicationVisibilityLab {

    enum RecordType {
        NORMAL,
        TRANSACTIONAL,
        COMMIT_MARKER
    }

    record LogRecord(
            int offset,
            String value,
            RecordType type
    ) {
    }

    static class Partition {

        final List<LogRecord> log = new ArrayList<>();

        final Map<String, Integer> replicaLeo = new LinkedHashMap<>();

        final Set<String> isr = Set.of("B1", "B2", "B3");

        final int firstOpenTransactionOffset = 2;

        int commitMarkerOffset = -1;

        Partition() {
            replicaLeo.put("B1", 0);
            replicaLeo.put("B2", 0);
            replicaLeo.put("B3", 0);
        }

        void append(String value, RecordType type) {
            int offset = log.size();

            log.add(new LogRecord(offset, value, type));

            // B1 — Leader. Его LEO увеличивается.
            replicaLeo.put("B1", log.size());
        }

        void appendCommitMarker() {
            commitMarkerOffset = log.size();

            append(
                    "T1_COMMIT",
                    RecordType.COMMIT_MARKER
                  );
        }

        int highWatermark() {
            // Вернуть минимальный LEO среди ISR.
            int result = replicaLeo.get("B1");
            for (String isrBroker : isr) {
                Integer endOffset = replicaLeo.get(isrBroker);
                result = Math.min(endOffset, result);
            }
            return result;
        }

        int lastStableOffset() {
            // Пока commit marker не достиг HW:
            // min(HW, firstOpenTransactionOffset)
            int hw = highWatermark();
            if (commitMarkerOffset != -1 && commitMarkerOffset < hw) {
                return hw;
            }
            return Math.min(hw, firstOpenTransactionOffset);
        }

        List<Integer> visibleOffsets(boolean readCommitted) {
            // Для read_uncommitted:
            // доступны пользовательские records,
            // у которых offset < HW.
            List<Integer> visibleOffsets = new ArrayList<>();
            // COMMIT_MARKER никогда не возвращаем.
            if (!readCommitted) {
                addBefore(highWatermark(), visibleOffsets);
            } else {
                // Для read_committed:
                // доступны records с offset < LSO.
                addBefore(lastStableOffset(), visibleOffsets);
            }
            return visibleOffsets;
        }

        private void addBefore(int bound, List<Integer> visibleOffsets) {
            for (LogRecord rec : log) {
                if (rec.offset >= bound) {
                    break;
                }
                if (rec.type != RecordType.COMMIT_MARKER) {
                    visibleOffsets.add(rec.offset);
                }
            }
        }

        void printState(String stage) {
            System.out.println("\n=== " + stage + " ===");

            System.out.println("Replica LEO: " + replicaLeo);

            System.out.println("HW: " + highWatermark());
            System.out.println("LSO: " + lastStableOffset());

            System.out.println(
                    "READ_UNCOMMITTED: "
                            + visibleOffsets(false)
                              );

            System.out.println(
                    "READ_COMMITTED:   "
                            + visibleOffsets(true)
                              );
        }
    }

    public static void main(String[] args) {

        Partition p = new Partition();

        p.append("OrderCreated(101)", RecordType.NORMAL);
        p.append("InventoryReserved(101)", RecordType.NORMAL);

        p.append("PaymentCompleted(101)", RecordType.TRANSACTIONAL);
        p.append("OrderCompleted(101)", RecordType.TRANSACTIONAL);

        p.append("OrderCreated(102)", RecordType.NORMAL);
        p.append("OrderCreated(103)", RecordType.NORMAL);

        // Сценарий 1: Followers отстают.
        p.replicaLeo.put("B2", 4);
        p.replicaLeo.put("B3", 4);

        p.printState("FOLLOWERS BEHIND");

        // Сценарий 2: Followers догнали Leader.
        p.replicaLeo.put("B2", 6);
        p.replicaLeo.put("B3", 6);

        p.printState("REPLICATION COMPLETED");

        // Сценарий 3: добавлен Commit Marker,
        // но он ещё не реплицирован.
        p.appendCommitMarker();

        p.printState("COMMIT MARKER NOT REPLICATED");

        // Сценарий 4: Commit Marker реплицирован.
        p.replicaLeo.put("B2", 7);
        p.replicaLeo.put("B3", 7);

        p.printState("TRANSACTION COMMITTED");
    }
}