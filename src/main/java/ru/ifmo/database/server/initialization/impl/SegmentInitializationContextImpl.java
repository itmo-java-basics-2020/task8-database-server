package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize;
    private final SegmentIndex index;

    private SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    @Override
    public String getSegmentName() {
        //todo
        return null;
    }

    @Override
    public Path getSegmentPath() {
        //todo
        return null;
    }

    @Override
    public SegmentIndex getIndex() {
        //todo
        return null;
    }

    @Override
    public int getCurrentSize() {
        //todo
        return 0;
    }
}