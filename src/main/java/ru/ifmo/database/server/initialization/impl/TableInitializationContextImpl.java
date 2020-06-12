package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {

    private final String name;
    private final Path path;
    private final TableIndex index;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.name = tableName;
        this.path = Path.of(databasePath.toString(), tableName);
        this.index = tableIndex;
        this.currentSegment = null;
    }

    @Override
    public String getTableName() {
        return this.name;
    }

    @Override
    public Path getTablePath() {
        return path;
    }

    @Override
    public TableIndex getTableIndex() {
        return this.index;
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
