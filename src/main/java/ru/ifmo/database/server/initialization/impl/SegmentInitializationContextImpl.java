package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize;
    private final SegmentIndex index;
    private final boolean isReadOnly;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index, boolean isReadOnly) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
        this.isReadOnly = isReadOnly;
    }

    @Override
    public String getSegmentName() {
        return this.segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return this.segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return this.index;
    }

    @Override
    public int getCurrentSize() {
        return this.currentSize;
    }

    public boolean isReadOnly() {
        return this.isReadOnly;
    }
}