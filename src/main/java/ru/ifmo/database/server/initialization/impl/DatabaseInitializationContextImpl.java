package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.Map;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

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
