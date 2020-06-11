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
    private final HashMap<String, Segment> tableSegments;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
        this.currentSegment = null;
        this.tableSegments = new HashMap<>();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return Path.of(databasePath.toString() + "\\" + tableName);
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

    @Override
    public Path getDatabasePath() {
        return databasePath;
    }

    @Override
    public void addSegment(String segmentName, Segment segment) {
        if (!tableSegments.containsKey(segmentName)) {
            tableSegments.put(segmentName, segment);
        }
    }

    @Override
    public Segment getSegment(String segmentName) {
        if (tableSegments.containsKey(segmentName)) {
            return tableSegments.get(segmentName);
        }
        return null;
    }

    @Override
    public Map<String, Segment> getSegments() {
        return tableSegments;
    }
}
