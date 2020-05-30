package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.SegmentIndexInfo;

public class SegmentIndexInfoImpl implements SegmentIndexInfo {

    private final long offset;

    public SegmentIndexInfoImpl(long offset) {
        this.offset = offset;
    }

    @Override
    public long getOffset() {
        throw new UnsupportedOperationException(); // todo implement
    }
}
