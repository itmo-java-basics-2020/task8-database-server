package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Database;

import java.util.Optional;


public class CreateTableCommand implements DatabaseCommand {

    private ExecutionEnvironment environment;
    private String DatabaseName;
    private String TableName;

    public CreateTableCommand(ExecutionEnvironment env, String[] args) {
        environment = env;

        DatabaseName = args[0];
        TableName = args[1];
    }

    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> databaseOptional = environment.getDatabase(DatabaseName);
        if (databaseOptional.isEmpty()) {
            return DatabaseCommandResult.error("Such database does not exist");
        }

        try {
            databaseOptional.get().createTableIfNotExists(TableName);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }

        return DatabaseCommandResult.success("Table was created successfully");
    }

}
