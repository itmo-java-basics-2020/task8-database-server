package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.logic.Segment;

import java.util.HashMap;
import java.util.Map;

public class TableIndex extends AbstractDatabaseIndex<String, Segment> {
    private final Map<String, Segment> tableMap;

    public TableIndex() {
        this.tableMap = new HashMap<>();
    }

    public Segment getSegment(String objectKey) {
        return tableMap.get(objectKey);
    }

    public void updateSegment(String objectKey, Segment segment) {
        tableMap.put(objectKey, segment);
    }

    public boolean ifContains(String objectKey) {
        return tableMap.containsKey(objectKey);
    }
}
