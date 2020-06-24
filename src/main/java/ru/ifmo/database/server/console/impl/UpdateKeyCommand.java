package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Database;

import java.util.Optional;

public class UpdateKeyCommand implements DatabaseCommand {

    private ExecutionEnvironment environment;
    private String DatabaseName;
    private String TableName;
    private String Key;
    private String Value;

    public UpdateKeyCommand(ExecutionEnvironment env, String... Args)
    {
        environment = env;


        DatabaseName = Args[0];
        TableName = Args[1];
        Key = Args[2];
        Value = Args[3];
    }

    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> databaseOptional = environment.getDatabase(DatabaseName);
        if (databaseOptional.isEmpty()) {
            return DatabaseCommandResult.error("Such database does not exist");
        }

        try {
            databaseOptional.get().write(TableName, Key, Value);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }

        return DatabaseCommandResult.success(Value);
    }

}

