package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import org.apache.commons.lang3.StringUtils;
import ru.ifmo.database.server.initialization.impl.*;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private final ExecutionEnvironment env;
    private static ExecutorService executor = Executors.newSingleThreadExecutor();


    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.env = env;
        if (this.env == null) {
            env = new ExecutionEnvironmentImpl();
        }
        File databaseServerDir = env.getWorkingPath().toFile();
        if (databaseServerDir.isDirectory()) {
            InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .build();
        } else {
            databaseServerDir.mkdir();
        }

    }

    public static void main(String[] args) throws IOException, DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        if (commandText == null || commandText.isEmpty()) {
            return DatabaseCommandResult.error("Command text must not be empty");
        }

        String[] args = commandText.split(" ");

        try {
            return DatabaseCommands.valueOf(args[0]).getCommand(env, args).execute();
        } catch (DatabaseException | IllegalArgumentException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        if (command == null) {
            return DatabaseCommandResult.error("Command must not be null");
        }

        try {
            return command.execute();
        } catch (DatabaseException | IllegalArgumentException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }
}
