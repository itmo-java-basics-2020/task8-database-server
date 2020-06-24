package ru.ifmo.database.server.index;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public class AbstractDatabaseIndex<K, V> implements DatabaseIndex<K, V> {

    private HashMap<K, V> map;

    protected AbstractDatabaseIndex() {
        this.map = new HashMap<>();
    }

    public Set<K> getKeys() {
        return map.keySet();
    }

    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        map.put(key, value);
    }

    @Override
    public Optional<V> searchForKey(K key) {
        return Optional.ofNullable(map.get(key));
    }
}