package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getTableName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public Path getTablePath() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public TableIndex getTableIndex() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public Segment getCurrentSegment() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        throw new UnsupportedOperationException(); // todo implement
    }
}
