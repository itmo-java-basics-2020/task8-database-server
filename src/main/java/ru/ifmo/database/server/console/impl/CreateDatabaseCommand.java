package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.DatabaseFactory;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

public class CreateDatabaseCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final DatabaseFactory databaseFactory;
    private final String databaseName;

    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory databaseFactory, String databaseName) {
        this.env = env;
        this.databaseFactory = databaseFactory;
        this.databaseName = databaseName;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        env.addDatabase(DatabaseImpl.create(databaseName, env.getWorkingPath()));
        return DatabaseCommandResult.success("Database " + databaseName + " was created successfully.");
    }
    /*
    private final ExecutionEnvironment env;
    private final DatabaseFactory databaseFactory;
    private final String databaseName;

    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseFactory = factory;
        this.databaseName = args[1];
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        env.addDatabase(databaseFactory.createNonExistent(databaseName, env.getWorkingPath()));
        return DatabaseCommandResult.success("Database: " + databaseName + "created");
    }*/
}
