package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    private final String dbName;
    private final Path dbRoot;
    private HashMap<String, Table> tables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.dbRoot = databaseRoot;
        this.tables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return this.dbName;
    }

    @Override
    public Path getDatabasePath() {
        return this.dbRoot;
    }

    @Override
    public HashMap<String, Table> getTables() {
        return this.tables;
    }

    @Override
    public void addTable(Table table) {
        this.tables.put(table.getName(), table);
    }
}
