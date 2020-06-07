package ru.ifmo.database.server;

import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.logic.Table;
import ru.ifmo.database.server.logic.impl.CachingTable;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CacheTest {

    @Mock
    public Table table;

    @Mock
    public DatabaseCache cache;

    @InjectMocks
    public CachingTable cachingTable = new CachingTable(table, cache);

    @Test
    public void test_DatabaseCache() {
        DatabaseCache cache = new DatabaseCache();
        String key = "key";
        String value = "value";

        cache.set(key, value);
        assertEquals(cache.get(key), value);
    }

    @Test
    @SneakyThrows
    public void test_usingCache() {
        String key = "key";
        String value = "value";
        when(cache.get(key)).thenReturn(value);

        assertEquals(cachingTable.read(key), value);
        verifyZeroInteractions(table);
    }

    @Test
    @SneakyThrows
    public void test_noCache() {
        String key = "key";
        String value = "value";
        when(table.read(key)).thenReturn(value);

        assertEquals(cachingTable.read(key), value);
        verify(cache).get(value);
    }

    @Test
    public void test_correctName() {
        String name = "name";
        when(table.getName()).thenReturn(name);

        assertEquals(cachingTable.getName(), name);
    }


    @Test
    @SneakyThrows
    public void test_writeToCache() {
        String key = "key";
        String value = "value";

        cachingTable.write(key, value);
        verify(cache).set(key, value);
        verify(table).write(key, value);
    }

}
