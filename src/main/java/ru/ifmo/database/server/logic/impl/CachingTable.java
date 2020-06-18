package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Table;

public class CachingTable implements Table {
    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this.table = table;
        this.cache = new DatabaseCache();
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
        try {
            cache.set(objectKey, objectValue);
            table.write(objectKey, objectValue);
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        String val = cache.get(objectKey);
        try {
            if (val == null) {
                String nonCacheValue = table.read(objectKey);
                cache.set(objectKey, nonCacheValue);
                return nonCacheValue;
            }
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
        return val;
    }
}