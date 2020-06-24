package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache extends LinkedHashMap<String, String> implements Cache {

    private static final int DEFAULT_CAPACITY = 100;
    private final int capacity;

    public DatabaseCache(int capacity) {
        super(capacity, 1f, true);
        this.capacity = capacity;
    }

    public DatabaseCache() {
        this(DEFAULT_CAPACITY);
    }

    @Override
    public String get(String key) {
        return super.get(key);
    }

    @Override
    public void set(String key, String value) {
        super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        return size() > capacity;
    }
}