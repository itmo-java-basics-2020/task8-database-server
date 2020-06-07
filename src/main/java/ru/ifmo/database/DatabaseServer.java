package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.context.InitializationContextImpl;
import ru.ifmo.database.server.initialization.impl.initializer.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.initializer.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.initializer.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.initializer.TableInitializer;

import java.io.File;

public class DatabaseServer {
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws DatabaseException {
        this.env = env;

        File databaseServerDir = env.getWorkingPath().toFile();
        if (databaseServerDir.isDirectory()) {
            InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .build();

            initializer.perform(initializationContext);
        } else {
            databaseServerDir.mkdir();
        }
    }

    public static void main(String[] args) throws DatabaseException {
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
        } catch (IllegalArgumentException | DatabaseException ex) {
            return DatabaseCommandResult.error(ex.getMessage());
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
