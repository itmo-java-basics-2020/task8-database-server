package ru.ifmo.database.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.ifmo.database.DatabaseServer;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.TableInitializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class YahimaInitializationTest {

    private static final String TEST_PATH = "TestDir";

    private String[] dbNames = new String[]{"db_1", "db_2"};
    private String[] tableNames = new String[]{"tb_1", "tb_2"};

    private Map<String, String> mapStorage = new ConcurrentHashMap<>();
    private Random random = new Random();
    private DatabaseServer databaseServer;

    public YahimaInitializationTest() throws DatabaseException, IOException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(Path.of(TEST_PATH)), initializer);
        createDataBases();
    }

    private void createDataBases() {
        ArrayList<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String key = generateAlphanumericString();
            String value = generateAlphanumericString();
            keys.add(key);
            mapStorage.put(key, value);
        }

        for (String dbName : dbNames) {
            databaseServer.executeNextCommand("CREATE_DATABASE " + dbName);
            for (String tableName : tableNames) {
                databaseServer.executeNextCommand("CREATE_TABLE " + dbName + " " + tableName);
                Collections.shuffle(keys);
                for (String key : keys) {
                    databaseServer.executeNextCommand("UPDATE_KEY " + dbName + " "
                            + tableName + " " + key + " " + mapStorage.get(key));
                }
            }
        }
    }

    @BeforeClass
    public static void createTestDir() {
        new File(TEST_PATH).mkdir();
    }

    @Test
    public void test_initOldDb() throws DatabaseException, IOException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(Path.of(TEST_PATH)), initializer);

        for (String dbName : dbNames) {
            for (String tableName : tableNames) {
                for (String key : mapStorage.keySet()) {
                    DatabaseCommandResult result = databaseServer.executeNextCommand(
                            "READ_KEY " + dbName + " " + tableName + " " + key + " " + mapStorage.get(key));

                    assertEquals(result.getResult(), Optional.of(mapStorage.get(key)));
                }
            }
        }
    }

    @AfterClass
    public static void clearTestDir() {
        deleteDirectoryRecursion(TEST_PATH);
    }

    private String generateAlphanumericString() {
        int leftLimit = 48;
        int rightLimit = 122;
        int targetStringLength = 10;

        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return generatedString;
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
