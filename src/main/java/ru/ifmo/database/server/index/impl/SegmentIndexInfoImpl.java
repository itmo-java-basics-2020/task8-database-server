package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.SegmentIndexInfo;

public class SegmentIndexInfoImpl implements SegmentIndexInfo {

    private final Integer offset;

    public SegmentIndexInfoImpl(Integer offset) {
        this.offset = offset;
    }

    @Override
    public Integer getOffset() {
        return offset;
    }
}
