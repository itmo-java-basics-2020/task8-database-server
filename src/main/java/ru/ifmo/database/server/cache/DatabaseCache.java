package ru.ifmo.database.server.cache;

import ru.ifmo.database.server.logic.Database;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {
    private static final int DEFAULT_SIZE = 100_000;

    private final LinkedHashMap<String, String> cache;

    public DatabaseCache() {
        cache = new LinkedHashMap<>(DEFAULT_SIZE, 1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > DEFAULT_SIZE;
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
