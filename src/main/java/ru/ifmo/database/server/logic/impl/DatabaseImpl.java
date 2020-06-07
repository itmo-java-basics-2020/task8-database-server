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
    private final Map<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName.contains("/") || dbName.contains("\\")) {
            throw new DatabaseException("Invalid database name. Must not have / or \\ characters");
        }

        this.dbName = dbName;
        this.tables = new HashMap<>();
        this.databaseRoot = databaseRoot;

        File databaseDir = new File(databaseRoot.toString(), dbName);
        if (databaseDir.isDirectory()) {
            throw new DatabaseException("Internal error. " +
                    "This database already exists, but it doesn't exists in environment");
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
    public boolean createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.get(tableName) != null) {
            return false;
        }

        Table table = TableImpl.create(tableName, Path.of(databaseRoot.toString(), dbName), new TableIndex());
        tables.put(table.getName(), table);
        return true;
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("Table '" +  tableName + "' doesn't exist");
        }

        table.write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("Table '" +  tableName + "' doesn't exist");
        }

        return table.read(objectKey);
    }
}
