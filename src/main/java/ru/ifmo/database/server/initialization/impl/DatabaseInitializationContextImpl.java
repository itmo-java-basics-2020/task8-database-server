package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    String dbName;
    Path databaseRoot;
    Map<String, Table> tables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        tables = new HashMap<>();
    }

    @Override
    public Path getDatabasePath() {
        return databaseRoot;
    }

    @Override
    public void addTable(Table table) {
        if (!tables.containsKey(table.getName())) {
            tables.put(table.getName(), table);
        }
    }
}
