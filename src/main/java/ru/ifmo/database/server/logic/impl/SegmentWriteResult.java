package ru.ifmo.database.server.logic.impl;


import ru.ifmo.database.server.logic.SegmentOperationResult;

public class SegmentWriteResult extends SegmentOperationResult {
    private final int notPrintedLength;


    private SegmentWriteResult(int notPrintedLength, OperationStatus status, String errorMessage, String resultReport) {
        super(status, errorMessage, resultReport);
        this.notPrintedLength = notPrintedLength;
    }

    public static SegmentWriteResult success() {
        return new SegmentWriteResult(0,
                OperationStatus.SUCCESS,
                null,
                "Everything was written");
    }

    public static SegmentWriteResult needMore(int notPrintedLength) {
        return new SegmentWriteResult(notPrintedLength,
                OperationStatus.NEED_MORE,
                null,
                "Need to print more");
    }

    public static SegmentWriteResult error(String message) {
        return new SegmentWriteResult(0,
                OperationStatus.ERROR,
                message,
                null);
    }

    public static SegmentWriteResult empty() {
        return needMore(-1);
    }

    public int getNotPrintedLength() {
        return notPrintedLength;
    }

}
