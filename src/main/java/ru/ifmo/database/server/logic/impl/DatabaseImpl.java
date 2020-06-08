package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializationContextImpl;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path dbRoot;
    private final Map<String, Table> tableMap;

    private DatabaseImpl(String dbName, Path dbRoot) throws DatabaseException {
        this.dbName = dbName;
        this.dbRoot = dbRoot;
        this.tableMap = new HashMap<>();

        File dbDir = new File(dbRoot.toString(), dbName);
        if (dbDir.isDirectory()) {
            throw new DatabaseException("Db already exists");
        }

        dbDir.mkdir();
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.dbRoot = context.getDatabasePath().getParent();
        this.tableMap = context.getTables();
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
        if (tableMap.get(tableName) != null) {
            return;
        }

        Table newTable = TableImpl.create(tableName, Path.of(dbRoot.toString(), dbName), new TableIndex());
        tableMap.put(tableName, newTable);
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        if (tableMap.get(tableName) != null) {
            return;
        }

        Table newTable = TableImpl.create(tableName, Path.of(dbRoot.toString(), dbName), new TableIndex(), segmentSizeInBytes);
        tableMap.put(tableName, newTable);
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        Table table = tableMap.get(tableName);
        try {
            table.write(objectKey, objectValue);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        Table table = tableMap.get(tableName);

        return table.read(objectKey);
    }
}
