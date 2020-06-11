package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRU<K, V> {

    private final LinkedHashMap<K, V> values;
    private final int maxCapacity;

    public LRU(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Capacity should be bigger than 0");
        }
        this.values = new LinkedHashMap<K, V>(capacity, 1f, true) {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > maxCapacity;
            }
        };
        this.maxCapacity = capacity;
    }

    public V get(K key) {
        return values.get(key);
    }

    public void put(K key, V value) {
        values.put(key, value);
    }

    public int elements() {
        return values.keySet().size();
    }
}
