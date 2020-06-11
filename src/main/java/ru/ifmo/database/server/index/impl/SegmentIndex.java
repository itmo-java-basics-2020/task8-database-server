package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.logic.Segment;

import java.util.HashMap;

public class SegmentIndex {
    private final HashMap<String, SegmentIndexInfo> segmentMap;

    public SegmentIndex() {
        this.segmentMap = new HashMap<>();
    }

    public boolean ifContains(String objectKey) {
        return segmentMap.containsKey(objectKey);
    }

    public SegmentIndexInfo getSegmentIndexInfo(String objectKey) {
        return segmentMap.get(objectKey);
    }

    public void updateSegmentMap(String objectKey, Integer offset) {
        segmentMap.put(objectKey, new SegmentIndexInfoImpl(offset));
    }
}
