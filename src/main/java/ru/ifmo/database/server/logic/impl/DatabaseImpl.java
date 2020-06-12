package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> databaseTables;

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        this.databaseTables = new HashMap<>();
        File dbDir = new File(databaseRoot.toString(), dbName);
        if (dbDir.isDirectory()) {
            throw new DatabaseException(dbName + " already exists");
        }
        if (!dbDir.mkdir()) {
            throw new DatabaseException("Can't create " + dbName);
        }
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.databaseRoot = context.getDatabasePath().getParent();
        this.databaseTables = context.getTables();
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    @Override
    public String getName() {
        return this.dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (this.databaseTables.get(tableName) == null) {
            Table table = TableImpl.create(tableName, Path.of(this.databaseRoot.toString(), tableName), new TableIndex());
            this.databaseTables.put(tableName, table);
        } else {
            throw new DatabaseException(tableName + " already exists");
        }
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        Table table = this.databaseTables.get(tableName);
        if (table != null) {
            table.write(objectKey, objectValue);
        } else {
            throw new DatabaseException(tableName + " doesn't exist");
        }
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        Table table = this.databaseTables.get(tableName);
        if (table != null) {
            return table.read(objectKey);
        } else {
            throw new DatabaseException(tableName + " doesn't exist");
        }
    }
}
