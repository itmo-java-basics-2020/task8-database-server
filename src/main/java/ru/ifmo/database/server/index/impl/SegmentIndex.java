package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.index.SegmentIndexInfo;

import java.util.HashMap;
import java.util.Map;

public class SegmentIndex extends AbstractDatabaseIndex<String, SegmentIndexInfo> {

    private final Map<String, SegmentIndexInfo> map;

    public SegmentIndex() {
        this.map = new HashMap<>();
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public SegmentIndexInfo getSegmentIndexObject(String key) {
        return map.get(key);
    }

    public void putSegmentMap(String key, Integer offset) {
        map.put(key, new SegmentIndexInfoImpl(offset));
    }
}
