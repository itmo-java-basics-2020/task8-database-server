package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {


    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getDbName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public Path getDatabasePath() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public Map<String, Table> getTables() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void addTable(Table table) {
        throw new UnsupportedOperationException(); // todo implement
    }
}
