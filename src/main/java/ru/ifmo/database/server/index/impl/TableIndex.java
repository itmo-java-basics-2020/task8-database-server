package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.logic.Segment;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class TableIndex extends AbstractDatabaseIndex<String, Segment> {

    public TableIndex() {
        map = new TreeMap<>();
    }

    public Optional<Segment> next(String currentSegmentName) {
        Map.Entry<String, Segment> entry = map.higherEntry(currentSegmentName);
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(entry.getValue());
    }
}
