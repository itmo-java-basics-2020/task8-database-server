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
    private final Map<String, Table> tables;

    public DatabaseImpl(DatabaseInitializationContext context) {
        dbName = context.getDbName();
        databasePath = context.getDatabasePath();
        tables = context.getTables();
    }

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        this.dbName = dbName;
        this.databasePath = Path.of(databaseRoot.toString() + File.separator + dbName);
        File dir = new File(String.valueOf(databasePath));
        if (dir.isDirectory()) {
            throw new DatabaseException("Directory with path: " + databasePath + " already exist");
        }
        if (!dir.mkdir()) {
            throw new DatabaseException("Can't create directory:" + dir.getAbsolutePath());
        }
        tables = new HashMap<>();
    }

    public static void main(String[] args) throws DatabaseException {
        DatabaseStoringUnit unit = new DatabaseStoringUnit("key_12345", "value_36918_6438_8858");
        System.out.println(unit.getValueSize());
//        Database database = create("DatabaseTest", Path.of("./"));
//        database.createTableIfNotExists("TableTest");
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) throws DatabaseException {
        return new DatabaseImpl(context);
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("table already exist");
        }
        Table tt = TableImpl.create(tableName, databasePath);
        Table t = new CachingTable(tt);
        tables.put(tableName, t);
    }


    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException();
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
