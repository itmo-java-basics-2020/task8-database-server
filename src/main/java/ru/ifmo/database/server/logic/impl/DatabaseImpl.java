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
    private final String dbName;
    private final Path databasePath;
    private final Map<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) {
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(databaseRoot);

        this.dbName = dbName;
        this.databasePath = databaseRoot.resolve(dbName);
        this.tables = new HashMap<>(16);
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.databasePath = context.getDatabasePath();
        this.tables = context.getTables();
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        DatabaseImpl db = new DatabaseImpl(dbName, databaseRoot);
        db.initializeAsNew();
        return db;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    private void initializeAsNew() throws DatabaseException {
        //todo
    }

    @Override
    public String getName() {
        //todo
        return null;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        //todo
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        //todo
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        //todo
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        //todo
        return null;
    }
}
