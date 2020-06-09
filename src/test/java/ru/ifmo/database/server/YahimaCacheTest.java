package ru.ifmo.database.server;

import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import ru.ifmo.database.DatabaseServer;
import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.TableInitializer;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;
import ru.ifmo.database.server.logic.impl.CachingTable;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class YahimaCacheTest {

    private static final String TEST_PATH = "TestDir";

    private static final String DB_NAME = "db_1";

    private static final String TABLE_NAME = "tb_1";

    private static final String KEY_NAME = "key";

    private static final String VALUE = "testValue";

    private static final String DB_PATH = TEST_PATH + "/" + DB_NAME;

    @Mock
    public ExecutionEnvironment env;

    @Mock
    public Database database;

    @Mock
    public Table mockTable;

    @Mock
    public DatabaseCache mockCache;

    public DatabaseServer server;

    public YahimaCacheTest() throws DatabaseException, IOException {
        MockitoAnnotations.initMocks(this);
        when(env.getWorkingPath()).thenReturn(Path.of(TEST_PATH));
        server = new DatabaseServer(env,
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
    }

    @Test
    public void test_cache() {
        DatabaseCache cache = new DatabaseCache();
        String key = "testKey";
        ArrayList<String> values = new ArrayList<>() {{
            add("testValue1");
            add("testValue2");
            add(null);
        }};

        for (String value : values) {
            cache.set(key, value);
            assertEquals(value, cache.get(key));
        }
    }

    @Test
    public void test_cachingTable() throws DatabaseException {
        CachingTable cachingTable = new CachingTable(mockTable, mockCache);
        when(mockTable.getName()).thenReturn(TABLE_NAME);

        cachingTable.write(KEY_NAME, VALUE);
        verify(mockTable).write(KEY_NAME, VALUE);
        verify(mockCache).set(KEY_NAME, VALUE);

        assertEquals(cachingTable.getName(), TABLE_NAME);
        verify(mockTable).getName();

        when(mockCache.get(KEY_NAME)).thenReturn(VALUE);
        assertEquals(VALUE, cachingTable.read(KEY_NAME));
        verifyNoMoreInteractions(mockTable);
    }

    @Test
    public void test_dbUsesCache() throws DatabaseException {
        new File(TEST_PATH).mkdir();
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        database.write(TABLE_NAME, KEY_NAME, VALUE);
        deleteDirectoryRecursion(DB_PATH);

        String value = database.read(TABLE_NAME, KEY_NAME);

        assertEquals(VALUE, value);
        deleteDirectoryRecursion(TEST_PATH);
    }

    private static void deleteDirectoryRecursion(String path) {
        File file = new File(path);
        if (file.isDirectory()) {
            for (File entry : file.listFiles()) {
                deleteDirectoryRecursion(entry.getPath());
            }
        }
        file.delete();
    }
}
