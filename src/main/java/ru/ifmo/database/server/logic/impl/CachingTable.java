package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.DatabaseCache;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Table;

public class CachingTable implements Table {
    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this(table, new DatabaseCache());
    }

    public CachingTable(Table table, DatabaseCache cache) {
        this.table = table;
        this.cache = cache;
    }

    @Override
    public String getName() {
        //todo
        return null;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        //todo
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        //todo
        return null;
    }
}
