package ru.ifmo.database.server.console;

import java.util.Optional;

public interface DatabaseCommandResult {

    Optional<String> getResult();

    DatabaseCommandStatus getStatus();

    static DatabaseCommandResult success(String result)
    {
        return new DatabaseCommandResultClass(DatabaseCommandStatus.SUCCESS, result, null);
    }

    static DatabaseCommandResult error(String result)
    {
        return new DatabaseCommandResultClass(DatabaseCommandStatus.FAILED, null, result);
    }

    boolean isSuccess();

    String getErrorMessage();

    enum DatabaseCommandStatus {
        SUCCESS, FAILED
    }
}

class DatabaseCommandResultClass implements DatabaseCommandResult {
    private final DatabaseCommandStatus status;
    private final String result;
    private final String message;

    DatabaseCommandResultClass(DatabaseCommandStatus status, String result, String message){
        this.status = status;
        this.result = result;
        this.message = message;
    }

    @Override
    public Optional<String> getResult() {
    	return Optional.ofNullable(this.result);
    }

    @Override
    public DatabaseCommandStatus getStatus() {
        return status;
    }

    @Override
    public boolean isSuccess() {
        return status == DatabaseCommandStatus.SUCCESS;
    }

    @Override
    public String getErrorMessage() {
        return message;
    }
}


