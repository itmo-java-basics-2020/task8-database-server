package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws DatabaseException {
        this.env = env;
        System.out.println(env);
        File dir = new File(env.getWorkingPath().toString());
        if (dir.isDirectory()) {
            InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .build();
            initializer.perform(initializationContext);
        } else {
            if (!dir.mkdir()) {
                throw new DatabaseException("Can't create directory for ExecutionEnvironment:" + env.getWorkingPath());
            }
        }


    }

    public static void main(String[] args) throws DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
//        databaseServer.executeNextCommand()
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        if (commandText == null) {
            return DatabaseCommandResult.error("Command == null");
        }
        String[] arguments = commandText.split(" ");
        if (arguments.length == 0) {
            return DatabaseCommandResult.error("Zero arguments were given");
        }
        try {
            return DatabaseCommands.valueOf(arguments[0]).getCommand(env, arguments).execute();
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        if (command == null) {
            return DatabaseCommandResult.error("Command == null");
        }
        try {
            return command.execute();
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
    }
}
