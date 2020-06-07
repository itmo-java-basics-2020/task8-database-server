package ru.ifmo.database.server.initialization.impl.context;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private final SegmentIndex index;
    private final String segmentName;
    private final Path segmentPath;
    private final long currentSize;
    private final boolean isSegmentReadOnly;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize,
                                            boolean isSegmentReadOnly, SegmentIndex index) {
        this.index = index;
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
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
    public boolean isSegmentReadOnly() {
        return isSegmentReadOnly;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }
}