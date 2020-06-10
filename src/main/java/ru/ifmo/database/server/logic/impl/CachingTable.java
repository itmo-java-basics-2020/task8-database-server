package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;

public class CachingTable implements Table {

    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this.table = table;
        cache = new DatabaseCache();
    }

    public CachingTable(Table table, DatabaseCache cache) {
        this.table = table;
        this.cache = cache;
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey, objectValue);
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        if (cache.get(objectKey) != null) return cache.get(objectKey);
        return table.read(objectKey);
    }
}
