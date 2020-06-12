package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private final String name;
    private final Path path;
    private final int currentSize;
    private final SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.name = segmentName;
        this.path = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    @Override
    public String getSegmentName() {
        return this.name;
    }

    @Override
    public Path getSegmentPath() {
        return this.path;
    }

    @Override
    public SegmentIndex getIndex() {
        return this.index;
    }

    @Override
    public int getCurrentSize() {
        return this.currentSize;
    }
}