package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.DatabaseCache;
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
        return this.table.getName();
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        this.cache.set(objectKey, objectValue);
        this.table.write(objectKey, objectValue);
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        String objectValue = this.cache.get(objectKey);

        if (objectValue == null) {
            String nonCacheValue = this.table.read(objectKey);
            this.cache.set(objectKey, nonCacheValue);
            return nonCacheValue;
        }

        return objectValue;
    }
}
