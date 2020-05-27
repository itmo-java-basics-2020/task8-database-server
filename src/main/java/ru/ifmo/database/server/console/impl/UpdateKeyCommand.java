package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Database;

import java.util.Optional;

public class UpdateKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public UpdateKeyCommand(ExecutionEnvironment env, String... args) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args[1];
        this.tableName = args[2];
        this.key = args[3];
        this.value = args[4];
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            throw new DatabaseException("No such database: " + databaseName);
        }
        String prevValue = database.get().read(tableName, key);
        database.get().write(tableName, key, value);
        return DatabaseCommandResult.success(prevValue);
    }
}
