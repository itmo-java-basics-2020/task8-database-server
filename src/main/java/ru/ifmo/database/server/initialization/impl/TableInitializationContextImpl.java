package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {

    @Override
    public String getTableName() {
        //todo
        return null;
    }

    @Override
    public Path getTablePath() {
        //todo
        return null;
    }

    @Override
    public TableIndex getTableIndex() {
        //todo
        return null;
    }

    @Override
    public Segment getCurrentSegment() {
        //todo
        return null;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        //todo
    }
}
