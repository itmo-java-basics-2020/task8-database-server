package ru.ifmo.database.server.console;

import ru.ifmo.database.server.exception.DatabaseException;

public interface DatabaseCommand {
    DatabaseCommandResult execute() throws DatabaseException;
}
