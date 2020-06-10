package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DatabaseInputStream extends DataInputStream {
    private static final int DEFAULT_LENGTH = 4;


    public DatabaseInputStream(InputStream inputStream) throws IOException {
        super(inputStream);
    }

    public DatabaseInputStream(InputStream inputStream, long offset) throws IOException {
        super(inputStream);
        skipNBytes(offset);
    }


    public SegmentReadResult readDbUnit() throws IOException {
        byte[] key = readDbUnitPart();
        byte[] value = readDbUnitPart();
        byte[] result = mergeArrays(key, value);
        return SegmentReadResult.needMore(result);
    }

    public SegmentReadResult readDbUnit(SegmentReadResult previousPart) throws IOException {
        byte[] keySize = previousPart.getKeySizeBytes();
        byte[] key = previousPart.getKeyBytes();
        byte[] valueSize = previousPart.getValueSizeBytes();
        byte[] value = previousPart.getValueBytes();

        int keyLength = keySize.length == 4 ? ByteBuffer.wrap(keySize).getInt() : -1;
        int valueLength = valueSize.length == 4 ? ByteBuffer.wrap(valueSize).getInt() : -1;

        byte[] newKeySize = new byte[0];
        byte[] newKey = new byte[0];
        byte[] newValueSize = new byte[0];
        byte[] newValue = new byte[0];

        if (!previousPart.isKeySizeRead()) {
            newKeySize = in.readNBytes(DEFAULT_LENGTH - keySize.length);
        }
        if (!previousPart.isKeyRead() && previousPart.isKeySizeRead()) {
            newKey = in.readNBytes(keyLength - key.length);
        }
        if (!previousPart.isValueSizeRead() && previousPart.isKeyRead()) {
            newValueSize = in.readNBytes(DEFAULT_LENGTH - valueSize.length);
        }
        if (!previousPart.isValueRead() && previousPart.isValueSizeRead()) {
            newValue = in.readNBytes(valueLength - value.length);
        }
        return SegmentReadResult.needMore(mergeArrays(newKeySize, newKey, newValueSize, newValue));
    }

    private byte[] readDbUnitPart() throws IOException {
        byte[] byteLength = in.readNBytes(DEFAULT_LENGTH);
        if (byteLength.length != 4) {
            return byteLength;
        }
        int length = ByteBuffer.wrap(byteLength).getInt();
        byte[] value = in.readNBytes(length);
        return mergeArrays(byteLength, value);

    }


    public static byte[] mergeArrays(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }
        byte[] result = new byte[totalLength];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }
        return result;
    }

}
