package ru.ifmo.database.server;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ru.ifmo.database.DatabaseServer;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.TableInitializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(JUnit4.class)
public class DatabaseServerTest {

    private Random random = new Random();

    @Before
    @SneakyThrows
    public void clearStorageFolder() {
        FileUtils.deleteDirectory((new ExecutionEnvironmentImpl()).getWorkingPath().toFile());
    }

    @Test
    public void checkStorageCorrectness() throws IOException, DatabaseException {
        Map<String, String> mapStorage = new ConcurrentHashMap<>();
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        String dbName = "test_" + new Random().nextInt(1_000_000);
        String tableName = "table";
        String[] initCommands = {
                "CREATE_DATABASE " + dbName,
                "CREATE_TABLE " + dbName + " " + tableName
        };

        System.out.println(Arrays.toString(initCommands));

        Arrays.stream(initCommands)
                .forEach(databaseServer::executeNextCommand);

        List<String> allowedKeys = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "test_key_" + i)
                .limit(10_000)
                .collect(Collectors.toList());

        Collections.shuffle(allowedKeys);

        for (int i = 0; i < 300_000; i++) {
            DatabaseCommands commandType = random.nextDouble() > 0.9 ? DatabaseCommands.UPDATE_KEY : DatabaseCommands.READ_KEY;

            String key = allowedKeys.get(random.nextInt(allowedKeys.size()));

            switch (commandType) {
                case UPDATE_KEY: {

                    String value = key + "_" + i;
                    databaseServer.executeNextCommand(
                            "UPDATE_KEY " + dbName + " " + tableName + " " + key + " " + value);
                    mapStorage.put(key, value);

                    break;
                }
                case READ_KEY: {
                    if (!mapStorage.containsKey(key))
                        break;

                    DatabaseCommandResult commandResult = databaseServer.executeNextCommand(
                            "READ_KEY " + dbName + " " + tableName + " " + key);

                    if (commandResult.isSuccess()) {
                        Assert.assertEquals(mapStorage.get(key), commandResult.getResult().get());
                    }
                    else {
                        Assert.fail(commandResult.getErrorMessage() + " " + key);
                    }
                    break;
                }
            }
        }
    }

    @Test
    public void checkStorageCorrectnessManyMany() throws IOException, DatabaseException {
        Map<String, String> mapStorage = new ConcurrentHashMap<>();
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        DatabaseServer server = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        List<String> dbNames = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "db_" + i)
                .limit(10)
                .collect(Collectors.toList());
        List<String> tablesNames = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "table_" + i)
                .limit(10)
                .collect(Collectors.toList());
        dbNames.forEach(dbName -> {
            server.executeNextCommand("CREATE_DATABASE " + dbName);
            tablesNames.forEach(tableName ->
                    server.executeNextCommand("CREATE_TABLE " + dbName + " " + tableName));
        });

        List<NoteInfo> data = Stream.generate(() -> random.nextInt(100_000_000))
                .map(i -> new NoteInfo(
                        dbNames.get(random.nextInt(10)),
                        tablesNames.get(random.nextInt(10)),
                        "key_" + i,
                        "value_" + i))
                .limit(100_000)
                .collect(Collectors.toList());

        Collections.shuffle(data);

        for (int i = 0; i < 300_000; i++) {
            DatabaseCommands commandType = random.nextDouble() > 0.9 ? DatabaseCommands.UPDATE_KEY : DatabaseCommands.READ_KEY;

            NoteInfo note = data.get(random.nextInt(data.size()));

            switch (commandType) {
                case UPDATE_KEY: {
                    String value = note.getValue() + "_" + i;
                    note.setValue(value);

                    server.executeNextCommand(
                            "UPDATE_KEY " + note.getDb() + " " + note.getTable() +
                                    " " + note.getKey() + " " + value);
                    mapStorage.put(note.getDb() + note.getTable() + note.getKey(), value);

                    break;
                }
                case READ_KEY: {
                    if (!mapStorage.containsKey(note.getDb() + note.getTable() + note.getKey()))
                        break;

                    DatabaseCommandResult commandResult = server.executeNextCommand(
                            "READ_KEY " + note.getDb() + " " + note.getTable() + " " + note.getKey());

                    if (commandResult.isSuccess()) {
                        Assert.assertEquals(mapStorage.get(note.getDb() + note.getTable() + note.getKey()), commandResult.getResult().get());
                    }
                    else {
                        Assert.fail(commandResult.getErrorMessage() + " " + note.getKey());
                    }
                    break;
                }
            }
        }
    }

    //long keys and values about 1000 symbols
    @Test
    public void checkStorageCorrectnessLong() throws IOException, DatabaseException {
        Map<String, String> mapStorage = new ConcurrentHashMap<>();
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        DatabaseServer server = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        List<String> dbNames = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "db_" + i)
                .limit(10)
                .collect(Collectors.toList());
        List<String> tablesNames = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "table_" + i)
                .limit(10)
                .collect(Collectors.toList());
        dbNames.forEach(dbName -> {
            server.executeNextCommand("CREATE_DATABASE " + dbName);
            tablesNames.forEach(tableName ->
                    server.executeNextCommand("CREATE_TABLE " + dbName + " " + tableName));
        });

        List<NoteInfo> data = Stream.generate(() -> random.nextInt(100_000_000))
                .map(i -> new NoteInfo(
                        dbNames.get(random.nextInt(10)),
                        tablesNames.get(random.nextInt(10)),
                        "key_" + i + generateLongString(1_000),
                        "value_" + i + generateLongString(1_000)))
                .limit(1000)
                .collect(Collectors.toList());

        Collections.shuffle(data);

        for (int i = 0; i < 3_000; i++) {
            DatabaseCommands commandType = random.nextDouble() > 0.9 ? DatabaseCommands.UPDATE_KEY : DatabaseCommands.READ_KEY;

            NoteInfo note = data.get(random.nextInt(data.size()));

            switch (commandType) {
                case UPDATE_KEY: {
                    String value = note.getValue() + "_" + i;
                    note.setValue(value);

                    server.executeNextCommand(
                            "UPDATE_KEY " + note.getDb() + " " + note.getTable() +
                                    " " + note.getKey() + " " + value);
                    mapStorage.put(note.getDb() + note.getTable() + note.getKey(), value);

                    break;
                }
                case READ_KEY: {
                    if (!mapStorage.containsKey(note.getDb() + note.getTable() + note.getKey()))
                        break;

                    DatabaseCommandResult commandResult = server.executeNextCommand(
                            "READ_KEY " + note.getDb() + " " + note.getTable() + " " + note.getKey());

                    if (commandResult.isSuccess()) {
                        Assert.assertEquals(mapStorage.get(note.getDb() + note.getTable() + note.getKey()), commandResult.getResult().get());
                    }
                    else {
                        Assert.fail(commandResult.getErrorMessage() + " " + note.getKey());
                    }
                    break;
                }
            }
        }
    }

    @Test
    public void test_initializationOneOne() {
        testInitialization(1, 1, 100_000);
    }

    @Test
    public void test_initializationOneMany() {
        testInitialization(1, 100, 100_000);
    }

    @Test
    public void test_initializationManyOne() {
        testInitialization(100, 1, 100_000);
    }

    @Test
    public void test_initializationManyMany() {
        testInitialization(10, 10, 100_000);
    }

    @Test
    public void test_initializationManyManyHard() {
        testInitialization(25, 25, 1_000_000);
    }

    @SneakyThrows
    private void testInitialization(int countDb, int countTables, int countNotes) {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        DatabaseServer server = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        List<String> dbNames = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "db_" + i)
                .limit(countDb)
                .collect(Collectors.toList());
        List<String> tablesNames = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "table_" + i)
                .limit(countTables)
                .collect(Collectors.toList());
        dbNames.forEach(dbName -> {
            server.executeNextCommand("CREATE_DATABASE " + dbName);
            tablesNames.forEach(tableName ->
                server.executeNextCommand("CREATE_TABLE " + dbName + " " + tableName));
        });

        List<NoteInfo> data = Stream.generate(() -> random.nextInt(100_000_000))
                .map(i -> new NoteInfo(
                        dbNames.get(random.nextInt(countDb)),
                        tablesNames.get(random.nextInt(countTables)),
                        "key_" + i,
                        "value_" + i))
                .limit(countNotes)
                .collect(Collectors.toList());

        data.forEach(kv -> server.executeNextCommand(
                "UPDATE_KEY " + kv.getDb() + " " + kv.getTable() + " " + kv.getKey() + " " + kv.getValue()));


        DatabaseServer server2 = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        data.forEach(note ->
                Assert.assertEquals(
                        server2.executeNextCommand("READ_KEY " + note.getDb() + " " + note.getTable() + " " + note.getKey())
                                .getResult().get(),
                        note.getValue()));
    }

    public String generateLongString(int length) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    @Getter
    @Setter
    static class NoteInfo {

        private String db;
        private String table;
        private String key;
        private String value;

        NoteInfo(String db, String table, String key, String value) {
            this.db = db;
            this.table = table;
            this.key = key;
            this.value = value;
        }
    }
}