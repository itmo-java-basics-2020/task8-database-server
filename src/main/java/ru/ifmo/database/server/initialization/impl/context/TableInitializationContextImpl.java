package ru.ifmo.database.server.initialization.impl.context;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {

    private final String tableName;
    private final Path databasePath;
    private final TableIndex tableIndex;
    private Segment currentSegment = null;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.databasePath = databasePath;
    }

    @Override
    public String getTableName() {
        return tableName;
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
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        this.currentSegment = segment;
    }
}
