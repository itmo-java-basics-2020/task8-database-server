package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;

public class DatabaseImpl implements Database {

    private final String name;
    private final Path directory;
    private HashMap<String, Table> tables;

    private DatabaseImpl(String name, Path root) throws DatabaseException {
        this.name = Objects.requireNonNull(name);
        Path directory = Path.of(root.toString(), name);
        if (Files.exists(directory)) {
            throw new DatabaseException("Database " + name + " already exists");
        } else {
            try {
                this.directory = Files.createDirectory(directory);
            } catch (IOException ex) {
                throw new DatabaseException(
                        "Database " + name + " cannot be created: " + ex.getMessage(),
                        ex
                );
            }
        }
        this.tables = new HashMap<>();
    }

    private DatabaseImpl(String name, Path root, HashMap<String, Table> tables) {
        this.name = name;
        this.directory = root;
        this.tables = tables;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(
                context.getDbName(),
                context.getDatabasePath(),
                context.getTables()
        );
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.get(tableName) != null) {
            throw new DatabaseException("Table " + tableName + " already exists");
        }
        Table table = TableImpl.create(tableName, this.directory, new TableIndex());
        tables.put(tableName, new CachingTable(table));
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        Table table = this.getTable(tableName);
        table.write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        Table table = this.getTable(tableName);
        return table.read(objectKey);
    }

    private Table getTable(String tableName) throws DatabaseException {
        Table table = this.tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("No such table: " + tableName);
        }
        return table;
    }
}
