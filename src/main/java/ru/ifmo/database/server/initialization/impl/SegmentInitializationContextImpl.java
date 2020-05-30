package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getSegmentName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public Path getSegmentPath() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public SegmentIndex getIndex() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public int getCurrentSize() {
        throw new UnsupportedOperationException(); // todo implement
    }
}