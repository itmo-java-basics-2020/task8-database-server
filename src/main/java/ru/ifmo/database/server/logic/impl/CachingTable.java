package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Table;

public class CachingTable implements Table {
    public CachingTable(Table table) {
        throw new UnsupportedOperationException(); // todo implement
    }

    public CachingTable(Table table, DatabaseCache cache) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }
}
