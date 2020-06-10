package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class DatabaseImpl implements Database {

    private final String dbName;
    private final Path dbRoot;
    private final Map<String, Table> dbTables;
    private final DatabaseCache tableCache = new DatabaseCache(100);

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) throws DatabaseException{
        return new DatabaseImpl(context);
    }

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {

        this.dbName = dbName;
        this.dbRoot = databaseRoot;
        this.dbTables = new HashMap<>();

        try {
            Files.createDirectory(databaseRoot);
        } catch (IOException e) {
            throw new DatabaseException(String.format("Database \"%s\" already exists", dbName));
        }
    }

    private DatabaseImpl(DatabaseInitializationContext context) throws DatabaseException {
        this.dbName = context.getDbName();
        this.dbRoot = context.getDatabasePath();
        this.dbTables = context.getTables();
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        try {
            if (dbTables.containsKey(tableName)) {
                throw new DatabaseException("Table already exist");
            }
            Table newTable = TableImpl.create(tableName, dbRoot, new TableIndex());
            dbTables.put(tableName, newTable);
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        if (!dbTables.containsKey(tableName)) {
            throw new DatabaseException("No such table");
        } else {
            tableCache.set(objectKey, objectValue);
            dbTables.get(tableName).write(objectKey, objectValue);
        }
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        String value = tableCache.get(objectKey);
        if (value == null) {
            value = dbTables.get(tableName).read(objectKey);
            tableCache.set(objectKey, value);
        }
        return value;
    }
}
