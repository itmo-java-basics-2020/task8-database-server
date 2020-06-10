package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.env = env;
        InitializationContextImpl initializationContext = InitializationContextImpl.builder() // example using lombok @Builder
                .executionEnvironment(env)
                .build();
        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws IOException, DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        if (commandText == null) {
            return DatabaseCommandResult.error("Invalid command");
        }

        String[] args = commandText.split(" ");
        if (args.length == 0) {
            return DatabaseCommandResult.error("Invalid command");
        }

        try {
            return DatabaseCommands.valueOf(args[0]).getCommand(env, args).execute();
        } catch (IllegalArgumentException | DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        if (command == null) {
            return DatabaseCommandResult.error("Invalid command");
        }

        try {
            return command.execute();
        } catch (IllegalArgumentException | DatabaseException ex) {
            return DatabaseCommandResult.error(ex.getMessage());
        }
    }
}
