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
import java.util.Objects;

public class DatabaseImpl implements Database {

    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        this.tables = new HashMap<>();

        File databaseDir = new File(databaseRoot.toString(), dbName);
        if (databaseDir.isDirectory()) {
            throw new DatabaseException("Database \"" + dbName + "\" is already exists");
        }
        databaseDir.mkdir();
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.tables = context.getTables();
        this.databaseRoot = context.getDatabasePath().getParent();
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.get(tableName) != null) {
            throw new DatabaseException("Table is already exists");
        }

        Table table = TableImpl.create(tableName, Path.of(databaseRoot.toString(), dbName), new TableIndex());
        tables.put(table.getName(), table);
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table \"" + tableName + "\" is not exists");
        }

        tables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table \"" + tableName + "\" is not exists");
        }

        return tables.get(tableName).read(objectKey);
    }
}
