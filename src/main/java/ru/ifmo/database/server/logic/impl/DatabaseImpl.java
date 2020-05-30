package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseImpl implements Database {

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }
}
