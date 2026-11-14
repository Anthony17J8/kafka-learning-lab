package dev.kafka.simulator;

import java.util.ArrayList;
import java.util.List;

public class OffsetIndex {

    private final List<IndexEntry> entries = new ArrayList<>();

    public void append(int relativeOffset, int positionBytes) {
        // Проверить, что значения неотрицательные.
        if (relativeOffset < 0 || positionBytes < 0) {
            throw new IllegalArgumentException("relativeOffset and positionBytes cannot be negative");
        }

        // Если индекс не пуст:
        //   relativeOffset должен быть строго больше
        //   последнего сохранённого relativeOffset.
        //
        //   positionBytes тоже должен быть строго больше
        //   последней физической позиции.
        if (!isEmpty()) {
            if (relativeOffset <= entries.getLast().relativeOffset()
                    || positionBytes <= entries.getLast().positionBytes()
            ) {
                throw new IllegalStateException("relativeOffset and positionBytes must be in increasing order");
            }
        }

        // Добавить новую IndexEntry.
        entries.add(new IndexEntry(relativeOffset, positionBytes));
    }

    public IndexEntry floor(long targetRelativeOffset) {
        IndexEntry index = null;
        if (entries.isEmpty()) {
            return index;
        }

        // Бинарным поиском найти IndexEntry
        // с максимальным relativeOffset <= target.
        //
        // Если подходящей записи нет,
        // вернуть null.
        //
        // Сложность: O(log N).
        // Не использовать линейный перебор.
        int leftIdx = 0;
        int rightIdx = entries.size() - 1;
        while (leftIdx <= rightIdx) {
            int middle = leftIdx + ((rightIdx - leftIdx) / 2);
            IndexEntry candidate = entries.get(middle);
            if (candidate.relativeOffset() <= targetRelativeOffset) {
                index = candidate;
                leftIdx = middle + 1;
            } else {
                rightIdx = middle - 1;
            }
        }
        return index;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public List<IndexEntry> entries() {
        return List.copyOf(entries);
    }
}
