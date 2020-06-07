package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private final String segmentName;
    private final Path tableRootPath;
    private final Integer currentSize;
    private final SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path tableRootPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getTableRootPath() {
        return tableRootPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public int getCurrentSize() {
        return currentSize;
    }
}