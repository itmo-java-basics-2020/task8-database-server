package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    private String dbName;
    private Path databasePath;
    private Map<String, Table> tables = new HashMap<>(16);

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databasePath = databaseRoot.resolve(dbName);
    }

    @Override
    public String getDbName() {
        //todo
        return null;
    }

    @Override
    public Path getDatabasePath() {
        //todo
        return null;
    }

    @Override
    public Map<String, Table> getTables() {
        //todo
        return null;
    }

    @Override
    public void addTable(Table table) {
        //todo
    }
}
