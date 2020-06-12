package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {
    private static final int DEFAULT_CACHE_SIZE = 1000;
    private static final float DEFAULT_LOAD_FACTOR =  0.5f;

    private final Map<String, String> cache;

    public DatabaseCache(int capacity, float loadFactor) {
        this.cache = new LinkedHashMap<>(capacity, loadFactor) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > capacity;
            }
        };
    }

    public DatabaseCache() {
        this(DEFAULT_CACHE_SIZE, DEFAULT_LOAD_FACTOR);
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
