package ru.ifmo.database.server.index;

import java.util.Optional;

public class AbstractDatabaseIndex<K, V> implements DatabaseIndex<K, V> {

    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public Optional<V> searchForKey(K key) {
        throw new UnsupportedOperationException(); // todo implement
    }
}
