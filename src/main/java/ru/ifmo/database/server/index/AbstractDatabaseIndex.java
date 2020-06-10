package ru.ifmo.database.server.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AbstractDatabaseIndex<K, V> implements DatabaseIndex<K, V> {
    private final Map<K, V> map = new HashMap<>();

    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        map.put(key, value);
    }

    @Override
    public Optional<V> searchForKey(K key) {
        return Optional.ofNullable(map.get(key));
    }
}
