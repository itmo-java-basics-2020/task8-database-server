package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {
    private static final int DEFAULT_ENTRY_SIZE = 100_000;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private final int capacity;
    private final Map<String, String> cache;

    public DatabaseCache() {
        capacity = DEFAULT_ENTRY_SIZE;
        cache = new LinkedHashMap<>(capacity, DEFAULT_LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > capacity;
            }
        };
    }

    @Override
    public String get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, String value) {
        cache.put(key, value);
    }
}
