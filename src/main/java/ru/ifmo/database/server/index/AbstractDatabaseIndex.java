package ru.ifmo.database.server.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AbstractDatabaseIndex<K, V> implements DatabaseIndex<K, V> {

    protected final Map<K, V> map = new HashMap<>();

    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        this.map.put(key, value);
    }

    @Override
    public Optional<V> searchForKey(K key) {
        return Optional.ofNullable(this.map.get(key));
    }
}
