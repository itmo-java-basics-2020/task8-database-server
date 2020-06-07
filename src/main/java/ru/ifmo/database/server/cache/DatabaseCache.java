package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {

    private static final int DEFAULT_CACHE_SIZE = 100_000;
    private static final boolean DEFAULT_ACCESS_ORDER = true;
    private static final float DEFAULT_LOAD_FACTOR = (float) 0.75;

    private final LinkedHashMap<String, String> cache;

    public DatabaseCache() {
        cache = new LinkedHashMap<>(DEFAULT_CACHE_SIZE, DEFAULT_LOAD_FACTOR, DEFAULT_ACCESS_ORDER) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > DEFAULT_CACHE_SIZE;
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
