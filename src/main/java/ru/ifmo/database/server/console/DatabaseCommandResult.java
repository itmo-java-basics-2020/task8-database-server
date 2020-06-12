package ru.ifmo.database.server.console;

import java.util.Optional;

public interface DatabaseCommandResult {

    static DatabaseCommandResult success(String result) {
        return new DatabaseCommandResultImpl(DatabaseCommandStatus.SUCCESS, result, null);
    }

    static DatabaseCommandResult error(String message) {
        return new DatabaseCommandResultImpl(DatabaseCommandStatus.FAILED, null, message);
    }

    Optional<String> getResult();

    DatabaseCommandStatus getStatus();

    boolean isSuccess();

    String getErrorMessage();

    enum DatabaseCommandStatus {
        SUCCESS, FAILED
    }

    class DatabaseCommandResultImpl implements DatabaseCommandResult {
        private final DatabaseCommandStatus status;
        private final String result;
        private final String message;

        private DatabaseCommandResultImpl(DatabaseCommandStatus status, String result, String message) {
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
            return this.status;
        }

        @Override
        public boolean isSuccess() {
            return this.status == DatabaseCommandStatus.SUCCESS;
        }

        @Override
        public String getErrorMessage() {
            return this.message;
        }
    }

    /*
    static DatabaseCommandResult success(String result) {
        Objects.requireNonNull(result);
        return new DatabaseCommandResultImpl(result, null, DatabaseCommandStatus.SUCCESS);
    }

    static DatabaseCommandResult error(String message) {
        Objects.requireNonNull(message);
        return new DatabaseCommandResultImpl(null, message, DatabaseCommandStatus.FAILED);
    }

    static DatabaseCommandResult error(Exception exception) {
        Objects.requireNonNull(exception);
        String message = exception.getMessage() != null
            ? exception.getMessage()
            : Arrays.toString(exception.getStackTrace());
        return DatabaseCommandResult.error(message);
    }

    Optional<String> getResult();

    DatabaseCommandStatus getStatus();

    boolean isSuccess();

    String getErrorMessage();

    enum DatabaseCommandStatus {
        SUCCESS, FAILED
    }

    class DatabaseCommandResultImpl implements DatabaseCommandResult {
        private final String result;
        private final String errorMessage;
        private final DatabaseCommandStatus status;

        private DatabaseCommandResultImpl(String result, String errorMessage, DatabaseCommandStatus status) {
            this.result = result;
            this.errorMessage = errorMessage;
            this.status = status;
        }

        @Override
        public boolean isSuccess() {
            return status == DatabaseCommandStatus.SUCCESS;
        }

        @Override
        public DatabaseCommandStatus getStatus() {
            return status;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public Optional<String> getResult() {
            return Optional.ofNullable(result);
        }
    }*/
}