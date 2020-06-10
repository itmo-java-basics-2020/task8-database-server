package ru.ifmo.database.server.logic;

import java.util.Optional;

public abstract class SegmentOperationResult {
    protected static final int DEFAULT_KEY_SIZE = 4;
    protected static final int DEFAULT_VALUE_SIZE = 4;
    private OperationStatus status;
    private final String errorMessage;
    private final String resultReport;

    public enum OperationStatus {
        SUCCESS,
        NEED_MORE,
        ERROR
    }

    public SegmentOperationResult(OperationStatus status, String errorMessage, String resultReport) {
        this.status = status;
        this.errorMessage = errorMessage;
        this.resultReport = resultReport;
    }

    public OperationStatus getStatus() {
        return status;
    }

    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    public Optional<String> getResultReport() {
        return Optional.ofNullable(resultReport);
    }
}
