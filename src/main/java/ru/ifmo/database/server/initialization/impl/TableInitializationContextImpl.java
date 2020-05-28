package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {

    private final String tableName;
    private final Path tablePath;
    private final TableIndex tableIndex;
    private volatile Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = databasePath.resolve(tableName);
        this.tableIndex = tableIndex;
    }

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
