package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {

    private final String name;
    private final TableIndex tableIndex;
    private final Path pathToDatabaseRoot;
    private Segment currentSegment;

    private TableImpl(String name, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (name.contains("/") || name.contains("\\")) {
            throw new DatabaseException("Invalid table name. Must not have / or \\ characters");
        }

        this.name = name;
        this.tableIndex = tableIndex;
        this.pathToDatabaseRoot = pathToDatabaseRoot;

        File tableDir = new File(pathToDatabaseRoot.toString(), name);
        if (tableDir.isDirectory()) {
            throw new DatabaseException("Internal error. " +
                    "This table already exists, but it doesn't exists in database");
        }
        tableDir.mkdir();

        // We can't create segment without table dir
        this.currentSegment = createNewSegment();
    }

    private TableImpl(TableInitializationContext context) {
        this.name = context.getTableName();
        this.tableIndex = context.getTableIndex();
        this.pathToDatabaseRoot = context.getTablePath().getParent();
        this.currentSegment = context.getCurrentSegment();
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return new CachingTable(new TableImpl(tableName, pathToDatabaseRoot, tableIndex));
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new CachingTable(new TableImpl(context));
    }

    private Segment createNewSegment() throws DatabaseException {
        return SegmentImpl.create(SegmentImpl.createSegmentName(name), Path.of(pathToDatabaseRoot.toString(), name));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            if (!currentSegment.write(objectKey, objectValue)) {
                currentSegment = createNewSegment();
                if (!currentSegment.write(objectKey, objectValue)) {
                    throw new DatabaseException("The size of your data exceeds the default segment size");
                }
            }
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);

        } catch (IOException e) {
            throw new DatabaseException("Some exceptions with files: " + e.getMessage());
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<Segment> oSegment = tableIndex.searchForKey(objectKey);

        if (oSegment.isEmpty()) {
            throw new DatabaseException("Key '" + objectKey + "' doesn't exist in the table '" + name + "'");
        }

        try {
            return oSegment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Some exceptions with files: " + e.getMessage());
        }
    }
}