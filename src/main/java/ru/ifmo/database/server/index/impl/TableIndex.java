package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.logic.Segment;

import java.util.HashMap;

public class TableIndex {
    private final HashMap<String, Segment> tableMap;

    public TableIndex() {
        this.tableMap = new HashMap<>();
    }

    public boolean ifContains(String objectKey) {
        return tableMap.containsKey(objectKey);
    }

    public Segment getSegment(String objectKey) {
        return tableMap.get(objectKey);
    }

    public void updateSegment(String objectKey, Segment segment) {
        tableMap.put(objectKey, segment);
    }
}
