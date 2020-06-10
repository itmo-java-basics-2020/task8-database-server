package ru.ifmo.database.server.logic.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;



public class DatabaseStoringUnit {

    private final byte[] key;
    private final int keySize;

    private final byte[] value;
    private final int valueSize;

    public DatabaseStoringUnit(String objectKey, String objectValue) {
        this(objectKey.getBytes(), objectValue.getBytes());
    }

    public DatabaseStoringUnit(byte[] key, byte[] value) {
        this.key = key;
        keySize = key.length;
        this.value = value;
        valueSize = value.length;
    }

    public DatabaseStoringUnit(byte[] readData) {
        keySize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 0, 4)).getInt();
        valueSize = ByteBuffer.wrap(Arrays.copyOfRange(readData, 4 + keySize, 8 + keySize)).getInt();
        key = Arrays.copyOfRange(readData, 4, 4 + keySize);
        value = Arrays.copyOfRange(readData, 8 + keySize, 8 + keySize + valueSize);
    }

    public byte[] getKey() {
        return key;
    }

    public int getKeySize() {
        return keySize;
    }

    public byte[] getValue() {
        return value;
    }

    public int getValueSize() {
        return valueSize;
    }
}
