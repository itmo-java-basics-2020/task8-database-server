package ru.ifmo.database.server;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import ru.ifmo.database.DatabaseServer;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.TableInitializer;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static ru.ifmo.database.server.console.DatabaseCommandResult.DatabaseCommandStatus.FAILED;
import static ru.ifmo.database.server.console.DatabaseCommandResult.DatabaseCommandStatus.SUCCESS;

public class YahimaCommandsTest {

    private static final String TEST_PATH = "TestDir";

    private static final String DB_NAME = "db_1";

    private static final String TABLE_NAME = "tb_1";

    private static final String KEY_NAME = "key";

    private static final String VALUE = "testValue";

    private static final String DB_PATH = TEST_PATH + "/" + DB_NAME;

    @Mock
    public Database database;

    @Mock
    public ExecutionEnvironment env;

    public DatabaseServer server;

    public YahimaCommandsTest() throws DatabaseException, IOException {
        MockitoAnnotations.initMocks(this);
        when(env.getWorkingPath()).thenReturn(Path.of(TEST_PATH));
        server = new DatabaseServer(env,
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
    }

    @BeforeClass
    public static void createTestDir() {
        new File(TEST_PATH).mkdir();
    }

    @Before
    public void deleteDbDir() {
        deleteDirectoryRecursion(DB_PATH);
    }

    // ================= create database tests =================

    @Test
    public void test_createDb_success() {
        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName(DB_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertTrue(new File(DB_PATH).exists());
    }

    @Test
    public void test_createDb_dbExists() {
        new File(DB_PATH).mkdir();
        assertTrue(new File(DB_PATH).exists());

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName(DB_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    @Test
    public void test_createDb_wrongName() {
        String testPath = TEST_PATH + "/WrongName/" + DB_NAME;
        deleteDirectoryRecursion(testPath);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName("WrongName/" + DB_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
        assertFalse(new File(testPath).exists());
    }

    // ================= create table tests =================

    @Test
    public void test_createTable_success() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    @Test
    public void test_createTable_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    @Test
    public void test_createTable_tableExists() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    // ================= update key tests =================

    @Test
    public void test_updateKey_success() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertEquals(VALUE, database.read(TABLE_NAME, KEY_NAME));
    }

    @Test
    public void test_updateKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    @Test
    public void test_updateKey_noSuchTable() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    // ================= read key tests =================

    @Test
    public void test_readKey_success() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);
        database.write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertEquals(Optional.of(VALUE), result.getResult());
    }

    @Test
    public void test_readKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    @Test
    public void test_readKey_noSuchTable() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    @Test
    public void test_readKey_noSuchKey() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(result.getErrorMessage() == null || result.getErrorMessage().isBlank());
    }

    @AfterClass
    public static void clearTestDir() {
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

    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    static class Command {
        private String name;
        private String dbName;
        private String tableName;
        private String key;
        private String value;

        @Override
        public String toString() {
            return Stream.of(name, dbName, tableName, key, value)
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(" "));
        }
    }
}