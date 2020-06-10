package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.*;

public class DatabaseImpl implements Database {

    private final String dbName;
    private final Path databasePath;
    private final HashMap<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        this.dbName = dbName;
        this.databasePath = Path.of(databaseRoot.toString() + File.separator + dbName);
        File dir = new File(String.valueOf(databasePath));
        if (dir.isDirectory()) {
            throw new DatabaseException("Directory with path: " + databasePath + " already exist");
        }
        tables = new HashMap<>();
    }

    public static void main(String[] args) throws DatabaseException {
        Database database = create("DatabaseTest", Path.of("./"));
        database.createTableIfNotExists("TableTest");
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.containsKey(tableName)) {
            return;
        }
        Table t = TableImpl.create(tableName, databasePath);
        tables.put(tableName, t);
    }


    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement (not necessary)
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("No such table: " + tableName);
        }
        Table table = tables.get(tableName);
        table.write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("No such table: " + tableName);
        }
        Table table = tables.get(tableName);
        return table.read(objectKey);
    }

}
