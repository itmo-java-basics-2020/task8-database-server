package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.DatabaseFactory;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

public class CreateDatabaseCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final DatabaseFactory DatabaseFactory;
    private final String DatabaseName;

    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory df, String dn) {
        environment = env;
	DatabaseFactory = df;
        DatabaseName = dn;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        environment.addDatabase(DatabaseImpl.create(DatabaseName, environment.getWorkingPath()));
        return DatabaseCommandResult.success("Database " + DatabaseName + " was created successfully.");
    }

}