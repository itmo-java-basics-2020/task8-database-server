package ru.ifmo.database;

import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.CreateDatabaseCommand;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import org.apache.commons.lang3.StringUtils;
import ru.ifmo.database.server.initialization.impl.*;
import ru.ifmo.database.server.logic.Database;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();


    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        // todo implement

        InitializationContextImpl initializationContext = InitializationContextImpl.builder() // example using lombok @Builder
                .executionEnvironment(env)
                .build();

    }

    public static void main(String[] args) throws IOException, DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        throw new UnsupportedOperationException();
    }


    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) throws DatabaseException {
        return command.execute();
    }
}