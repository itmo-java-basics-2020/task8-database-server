package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize;
    private final SegmentIndex index;
    private final boolean readOnly;
    private final int segmentSizeInBytes;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index, boolean readOnly) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
        this.readOnly = readOnly;
        this.segmentSizeInBytes = 100_000;
    }

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index, boolean readOnly, int segmentSizeInBytes) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
        this.readOnly = readOnly;
        this.segmentSizeInBytes = segmentSizeInBytes;
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
    public int getCurrentSize() {
        return currentSize;
    }

    @Override
    public boolean getReadOnly() {
        return readOnly;
    }

    @Override
    public int getSegmentSizeInBytes() {
        return segmentSizeInBytes;
    }
}