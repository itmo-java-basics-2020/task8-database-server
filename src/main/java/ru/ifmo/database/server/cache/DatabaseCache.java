package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {
    private final LinkedHashMap<String, String> lruMap;

    public DatabaseCache(int capacity) {
        this.lruMap = new LinkedHashMap<>(capacity, 1, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > capacity;
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
