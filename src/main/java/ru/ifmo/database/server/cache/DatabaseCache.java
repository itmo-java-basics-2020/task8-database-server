package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {
    private static final int DEFAULT_CACHE_CAPACITY = 100000;

    private final LinkedHashMap<String, String> lruMap;

    public DatabaseCache() {
        this.lruMap = new LinkedHashMap<>(DEFAULT_CACHE_CAPACITY, 1, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > DEFAULT_CACHE_CAPACITY;
            }
        };
    }

    @Override
    public String get(String key) {
        return lruMap.get(key);
    }

    @Override
    public void set(String key, String value) {
        lruMap.put(key, value);
    }
}
