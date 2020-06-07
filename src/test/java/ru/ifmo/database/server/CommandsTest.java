package ru.ifmo.database.server;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import ru.ifmo.database.DatabaseServer;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.TableInitializer;
import ru.ifmo.database.server.logic.Database;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static ru.ifmo.database.server.console.DatabaseCommandResult.DatabaseCommandStatus.FAILED;
import static ru.ifmo.database.server.console.DatabaseCommandResult.DatabaseCommandStatus.SUCCESS;

public class CommandsTest {

    private static final String DB_NAME = "db_1";

    private static final String TABLE_NAME = "tb_1";

    private static final String KEY_NAME = "key";

    private static final String VALUE = "value";

    private static final String PREV_VALUE = "prev_value";

    @Mock
    public Database database;

    @Mock
    public ExecutionEnvironment env;

    public Path path = Path.of("/");

    public DatabaseServer server;

    public CommandsTest() throws IOException, DatabaseException {
        MockitoAnnotations.initMocks(this);
        when(env.getWorkingPath()).thenReturn(path);
        server = new DatabaseServer(env,
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
    }

    // ================= update key tests =================

    @Test
    public void test_readKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName("table")
                .key("key")
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_readKey_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        when(database.read(TABLE_NAME, KEY_NAME)).thenReturn(VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    @Test
    public void test_readKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).read(TABLE_NAME, KEY_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    // ================= update key tests =================

    @Test
    public void test_updateKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName("table")
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_updateKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    @Test
    public void test_updateKey_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doNothing().when(database).write(TABLE_NAME, KEY_NAME, VALUE);
        when(database.read(TABLE_NAME, KEY_NAME)).thenReturn(PREV_VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    // ================= create table tests =================

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
    }

    @Test
    public void test_createTable_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doNothing().when(database).createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    @Test
    public void test_createTable_exception() throws DatabaseException {
        var message = "Table already exists";
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doThrow(new DatabaseException(message)).when(database).createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    @Test
    public void test_executeNext_noCommandName() {
        DatabaseCommandResult databaseCommandResult = server.executeNextCommand((String) null);
        assertEquals(FAILED, databaseCommandResult.getStatus());
    }

    @Test
    public void test_executeNext_noCommandFound() {
        DatabaseCommandResult databaseCommandResult = server.executeNextCommand("fake_command_name");
        assertEquals(FAILED, databaseCommandResult.getStatus());
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