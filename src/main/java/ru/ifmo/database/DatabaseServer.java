package ru.ifmo.database;

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

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;


    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.env = env;
        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();
        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws IOException, DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        ExecutionEnvironment e = new ExecutionEnvironmentImpl();
        DatabaseServer databaseServer = new DatabaseServer(e, initializer);
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("1");
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("2");
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("3");
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("4");
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("5");
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("6");
        databaseServer.env.getDatabase("db_35875").get().createTableIfNotExists("7");

    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        if (commandText == null || commandText.isEmpty()) {
            return DatabaseCommandResult.error(new IllegalArgumentException("You passed null as command or empty command"));
        }

        String[] terms = commandText.split(" ");
        try {
            return DatabaseCommands.valueOf(terms[0])
                    .getCommand(env, terms)
                    .execute();
        } catch (DatabaseException | IllegalArgumentException e) {
            return DatabaseCommandResult.error(e);
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        if (command == null) {
            return DatabaseCommandResult.error("You passed null");
        }
        try {
            return command.execute();
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
