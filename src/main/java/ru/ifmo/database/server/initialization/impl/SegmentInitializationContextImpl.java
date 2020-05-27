package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

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