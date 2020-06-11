package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.LRU;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databaseRoot;
    private final HashMap<String, Table> dbTables;
    private final LRU<String, String> cache = new LRU<>(5);

    private DatabaseImpl(String dbName, Path databaseRoot) {
        try {
            if (!Files.exists(Path.of(databaseRoot.toString()))) {
                Files.createDirectory(databaseRoot);
            }
        } catch (IOException e) {
            System.out.println(dbName + " already exist");
        } finally {
            this.dbName = dbName;
            this.databaseRoot = databaseRoot;
            this.dbTables = new HashMap<>();
        }
    }

    private DatabaseImpl(String dbName, Path databaseRoot, Map<String, Table> dbTables) {
        try {
            if (!Files.exists(Path.of(databaseRoot.toString()))) {
                Files.createDirectory(databaseRoot);
            }
        } catch (IOException e) {
            System.out.println(dbName + " already exist");
        } finally {
            this.dbName = dbName;
            this.databaseRoot = databaseRoot;
            this.dbTables = (HashMap<String, Table>) dbTables;
        }
    }

    public static Database create(String dbName, Path databaseRoot) {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context.getDbName(), context.getDatabasePath(), context.getTables());
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (dbTables.containsKey(tableName)) {
            throw new DatabaseException("Table already exist");
        }
        Table newTable = TableImpl.create(tableName, databaseRoot, new TableIndex());
        dbTables.put(tableName, newTable);
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        if (!dbTables.containsKey(tableName)) {
            throw new DatabaseException("No such table");
        } else {
            cache.put(objectKey, objectValue);
            dbTables.get(tableName).write(objectKey, objectValue);
        }
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        if (!dbTables.containsKey(tableName)) {
            throw new DatabaseException("No such table");
        }
        String value = cache.get(objectKey);
        if (value == null) {
            value = dbTables.get(tableName).read(objectKey);
            cache.put(objectKey, value);
        }
        return value;
    }

    @Override
    public void addTable(String tableName, Table table) {
        if (!dbTables.containsKey(tableName)) {
            dbTables.put(tableName, table);
        }
    }

    @Override
    public Table getTable(String tableName) {
        if (dbTables.containsKey(tableName)) {
            return dbTables.get(tableName);
        }
        return null;
    }
}
