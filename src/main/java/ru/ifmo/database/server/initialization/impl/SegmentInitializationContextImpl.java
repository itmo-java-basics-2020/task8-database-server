package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private final SegmentIndex segmentIndex;
    private final long currentSize;


    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, SegmentIndex segmentIndex, long currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.segmentIndex = segmentIndex;
        this.currentSize = currentSize;
    }


    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return segmentIndex;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }


}