package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.CreateDatabaseCommand;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.DatabaseFactory;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseImpl implements Database {

    private Path databaseRoot;
    private String dbName;
    TableImpl Table = new TableImpl();


    public void create(String dbName, Path databaseRoot) throws DatabaseException {
        this.databaseRoot = databaseRoot;
        this.dbName = dbName;

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
        Table.create(tableName, this.databaseRoot, null);
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        this.Table.write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        return this.Table.read(objectKey);
    }
}
