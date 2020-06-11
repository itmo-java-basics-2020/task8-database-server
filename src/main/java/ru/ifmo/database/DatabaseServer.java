package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws DatabaseException {
        this.env = env;

        InitializationContext initializationContext = InitializationContextImpl.builder() // example using lombok @Builder
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws DatabaseException, IOException {
        Files.createDirectory(Path.of("db_files"));
        /*Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);*/
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        if (commandText == null || commandText.isBlank()) {
            return DatabaseCommandResult.error("Command text must not be null or blank.");
        }
        String[] commandWithArgs = commandText.split(" ");
        String commandName = commandWithArgs[0];

        try {
            DatabaseCommands commandType = DatabaseCommands.valueOf(commandName);
            DatabaseCommand command = commandType.getCommand(env, commandWithArgs);
            return command.execute();
        } catch (Exception exception) {
            return DatabaseCommandResult.error(exception.getMessage());
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        throw new UnsupportedOperationException(); // todo implement
    }
}
