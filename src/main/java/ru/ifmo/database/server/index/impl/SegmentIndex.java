package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.index.SegmentIndexInfo;

import java.util.HashMap;
import java.util.Map;

public class SegmentIndex extends AbstractDatabaseIndex<String, SegmentIndexInfo> {
    private final Map<String, SegmentIndexInfo> segmentIndexMap;

    public SegmentIndex() {
        this.segmentIndexMap = new HashMap<>();
    }

    public void updateSegmentIndexMap(String objectKey, int offset) {
        segmentIndexMap.put(objectKey, new SegmentIndexInfoImpl(offset));
    }

    public boolean isContains(String objectKey) {
        return segmentIndexMap.containsKey(objectKey);
    }

    public SegmentIndexInfo getSegmentIndexInfo(String objectKey) {
        return segmentIndexMap.get(objectKey);
    }
}
