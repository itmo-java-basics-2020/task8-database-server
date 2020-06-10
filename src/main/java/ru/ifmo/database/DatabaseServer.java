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
    private final ExecutionEnvironment executionEnvironment;

    private static ExecutorService executor = Executors.newSingleThreadExecutor();


    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.executionEnvironment = env;

        File dir = env.getWorkingPath().toFile();

        if (dir.isDirectory()) {
            InitializationContextImpl init = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .build();

            initializer.perform(init);
        } else {
            dir.mkdir();
        }
    }

    public static void main(String[] args) throws IOException, DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        if (commandText == null) {
            return DatabaseCommandResult.error("Null command");
        }

        String[] commands = commandText.split(" ");
        if (commands.length == 0) {
            return DatabaseCommandResult.error("Empty command");
        }

        try {
            return DatabaseCommands.valueOf(commands[0]).getCommand(executionEnvironment, commands).execute();
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        if (command == null) {
            return DatabaseCommandResult.error("Null command");
        }

        try {
            return command.execute();
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }
}
