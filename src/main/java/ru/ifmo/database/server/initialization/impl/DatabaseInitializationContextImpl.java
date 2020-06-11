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
    private final HashMap<String, Table> dbTables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        this.dbTables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Path getDatabasePath() {
        return databaseRoot;
    }

    @Override
    public Map<String, Table> getTables() {
        return dbTables;
    }

    @Override
    public void addTable(Table table) {
        if (!dbTables.containsKey(table.getName())) {
            dbTables.put(table.getName(), table);
        }
    }
}
