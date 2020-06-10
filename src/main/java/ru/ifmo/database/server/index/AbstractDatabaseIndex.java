package ru.ifmo.database.server.index;

import java.util.Optional;
import java.util.TreeMap;

public class AbstractDatabaseIndex<K, V> implements DatabaseIndex<K, V> {
    protected final TreeMap<K, V> map = new TreeMap<>();


    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        map.put(key, value);
    }

    @Override
    public Optional<V> searchForKey(K key) {
        return Optional.ofNullable(map.get(key));
    }
}
