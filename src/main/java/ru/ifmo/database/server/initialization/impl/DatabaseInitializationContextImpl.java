package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> databaseTables = new HashMap<>();

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
    }

    @Override
    public String getDbName() {
        return this.dbName;
    }

    @Override
    public Path getDatabasePath() {
        return this.databaseRoot;
    }

    @Override
    public Map<String, Table> getTables() {
        return this.databaseTables;
    }

    @Override
    public void addTable(Table table) {
        String tableName = table.getName();
        if (!this.databaseTables.containsKey(tableName)) {
            this.databaseTables.put(tableName, table);
        }
    }
}
