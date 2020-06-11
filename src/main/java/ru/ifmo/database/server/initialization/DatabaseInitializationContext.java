package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;

public interface DatabaseInitializationContext {
    String getDbName();

    Path getDatabasePath();

    HashMap<String, Table> getTables();

    void addTable(Table table);
}

