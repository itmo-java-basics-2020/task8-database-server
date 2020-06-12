package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.index.SegmentIndexInfo;

public class SegmentIndex extends AbstractDatabaseIndex<String, SegmentIndexInfo> {

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
