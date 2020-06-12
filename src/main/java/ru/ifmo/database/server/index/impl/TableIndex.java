package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.logic.Segment;

import java.util.HashMap;
import java.util.Map;

public class TableIndex extends AbstractDatabaseIndex<String, Segment> {
    private final Map<String, Segment> map;

    public TableIndex() {
        this.map = new HashMap<>();
    }

    public boolean containsKey(String key) {
        return this.map.containsKey(key);
    }

    public Segment getSegment(String key) {
        return this.map.get(key);
    }

    public void putSegment(String key, Segment segment) {
        this.map.put(key, segment);
    }
}
