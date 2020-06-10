package ru.ifmo.database.server.logic.impl;

public class DatabaseStoringUnit {

    private final byte[] key;
    private final Integer keySize;

    private final byte[] value;
    private final Integer valueSize;

    public DatabaseStoringUnit(String objectKey, String objectValue) {
        this(objectKey.getBytes(), objectValue.getBytes());
    }

    public DatabaseStoringUnit(byte[] key, byte[] value) {
        this.key = key;
        keySize = key.length;
        this.value = value;
        valueSize = value.length;
    }

    public byte[] getKey() {
        return key;
    }

    public Integer getKeySize() {
        return keySize;
    }

    public byte[] getValue() {
        return value;
    }

    public Integer getValueSize() {
        return valueSize;
    }

    public int getFileSize() {
        return 4 + key.length + 4 + value.length;
    }
}
