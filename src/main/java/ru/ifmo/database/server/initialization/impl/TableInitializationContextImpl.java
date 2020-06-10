package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String name;
    private final Path databasePath;
    private final TableIndex tableIndex;
    private Segment lastSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.name = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
        this.lastSegment = null;
    }

    @Override
    public String getTableName() {
        return name;
    }

    @Override
    public Path getTablePath() {
        return databasePath;
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return lastSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        this.lastSegment = segment;
    }
}
