package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class TableInitializationContextImpl implements TableInitializationContext {

    private final String tableName;
    private final Path databasePath;
    private final TableIndex tableIndex;
    private final Map<String, Segment> segments;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
        this.segments = new HashMap<>();
        this.currentSegment = null;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return Path.of(databasePath.toString(), tableName);
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
    }
}
