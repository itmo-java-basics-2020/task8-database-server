package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.Cache;
import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Table;

public class CachingTable implements Table {

    private Table table;
    private Cache cache;

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
        return this.table.getName();
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        this.table.write(objectKey, objectValue);
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        String cacheValue = this.cache.get(objectKey);
        if (cacheValue != null) {
            return cacheValue;
        }
        return this.table.read(objectKey);
    }
}
