package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path databasePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
        this.currentSegment = null;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public Path getTablePath() {
        return Path.of(this.databasePath.toString() + "/" + this.tableName);
    }

    @Override
    public TableIndex getTableIndex() {
        return this.tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return this.currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        this.currentSegment = segment;
    }
}
