package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.logic.SegmentOperationResult;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static ru.ifmo.database.server.logic.SegmentOperationResult.OperationStatus.*;
import static ru.ifmo.database.server.logic.impl.MergeArrays.mergeArrays;


public class SegmentReadResult extends SegmentOperationResult {

    private final byte[] readData;

    private SegmentReadResult(OperationStatus status, String errorMessage, String resultReport, byte[] readData) {
        super(status, errorMessage, resultReport);
        this.readData = readData;
    }

    public static SegmentReadResult success(byte[] readData) {
        return new SegmentReadResult(SUCCESS,
                null,
                "Everything read", readData);
    }

    public static SegmentReadResult needMore(byte[] readData) {
        return new SegmentReadResult(NEED_MORE,
                null,
                "Need to read more", readData);
    }

    public static SegmentReadResult error(String message) {
        return new SegmentReadResult(ERROR,
                message,
                null, null);
    }


    public static SegmentReadResult merge(SegmentReadResult result1, SegmentReadResult result2) {
        if (result1.getStatus() == ERROR || result2.getStatus() == ERROR) {
            return error(result1.getErrorMessage().or(() -> Optional.of("")).get()
                    + result2.getErrorMessage().or(() -> Optional.of("")).get());
        }
        byte[] data = mergeArrays(result1.readData, result2.readData);
        if (data.length < 8) {
            return needMore(data);
        }
        int keyLength = ByteBuffer.wrap(Arrays.copyOfRange(data, 0, 4)).getInt();
        if (data.length < 8 + keyLength) {
            return needMore(data);
        }
        int valueLength = ByteBuffer.wrap(Arrays.copyOfRange(data, 4 + keyLength, 8 + keyLength)).getInt();
        if (data.length < 8 + keyLength + valueLength) {
            return needMore(data);
        }
        return success(data);
    }

    public static SegmentReadResult empty() {
        return needMore(new byte[0]);
    }

    public Optional<DatabaseStoringUnit> getUnit() {
        if (getStatus() != SUCCESS) return Optional.empty();
        return Optional.of(new DatabaseStoringUnit(readData));
    }

    public boolean isKeySizeRead() {
        return readData.length >= DEFAULT_KEY_SIZE;
    }

    public boolean isKeyRead() {
        if (isKeySizeRead()) {
            int keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, DEFAULT_KEY_SIZE)).getInt();
            return readData.length >= DEFAULT_KEY_SIZE + keySize;
        }
        return false;

    }

    public boolean isValueSizeRead() {
        if (isKeyRead()) {
            int keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, DEFAULT_KEY_SIZE)).getInt();
            return readData.length >= DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE + keySize;
        }
        return false;
    }

    public boolean isValueRead() {
        if (isValueSizeRead()) {
            int keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, DEFAULT_KEY_SIZE)).getInt();
            int valueSize = ByteBuffer.wrap(Arrays.copyOfRange(readData,
                    DEFAULT_KEY_SIZE + keySize,
                    DEFAULT_KEY_SIZE + keySize + DEFAULT_VALUE_SIZE)).getInt();
            return readData.length >= DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE + keySize + valueSize;
        }
        return false;
    }

    public byte[] getKeySizeBytes() {
        return Arrays.copyOfRange(readData, 0, Math.min(readData.length, DEFAULT_KEY_SIZE));
    }

    public byte[] getKeyBytes() {
        if (isKeySizeRead()) {
            int keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, DEFAULT_KEY_SIZE)).getInt();
            return Arrays.copyOfRange(readData, DEFAULT_KEY_SIZE, Math.min(readData.length, DEFAULT_KEY_SIZE + keySize));
        }
        return new byte[0];
    }

    public byte[] getValueSizeBytes() {
        if (isKeyRead()) {
            int keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, DEFAULT_KEY_SIZE)).getInt();
            return Arrays.copyOfRange(readData,
                    DEFAULT_KEY_SIZE + keySize,
                    Math.min(readData.length, DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE + keySize));
        }
        return new byte[0];
    }

    public byte[] getValueBytes() {
        if (isValueSizeRead()) {
            int keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, DEFAULT_KEY_SIZE)).getInt();
            return Arrays.copyOfRange(readData,
                    DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE + keySize,
                    readData.length);
        }
        return new byte[0];
    }

    public int getReadLength() {
        return readData.length;
    }

    public byte[] getReadData() {
        return readData;
    }
}
