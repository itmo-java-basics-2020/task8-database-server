package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private final String segmentName;
    private final Path segmentPath;
    private final Integer currentSize;
    private final SegmentIndex index;
    private final Boolean isSegmentReadOnly;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize,
                                            SegmentIndex index, Boolean isSegmentReadOnly) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
        this.isSegmentReadOnly = isSegmentReadOnly;
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
        return index;
    }

    @Override
    public Integer getCurrentSize() {
        return currentSize;
    }

    @Override
    public Boolean isSegmentReadOnly() {
        return isSegmentReadOnly;
    }
}