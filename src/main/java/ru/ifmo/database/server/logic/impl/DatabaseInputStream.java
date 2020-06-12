package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static ru.ifmo.database.server.logic.impl.MergeArrays.mergeArrays;

public class DatabaseInputStream extends DataInputStream {
    private static final int DEFAULT_LENGTH = 4;


    public DatabaseInputStream(InputStream inputStream)  {
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
        return SegmentReadResult.merge(SegmentReadResult.empty(), SegmentReadResult.needMore(result));
    }

    public SegmentReadResult readDbUnit(SegmentReadResult previousPart) throws IOException {
        if (previousPart == null) {
            return readDbUnit();
        }
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

        // reading keySize;
        if (!previousPart.isKeySizeRead()) {
            newKeySize = in.readNBytes(DEFAULT_LENGTH - keySize.length);
            previousPart = SegmentReadResult.merge(previousPart, SegmentReadResult.needMore(newKeySize));
            keySize = previousPart.getKeySizeBytes();
            keyLength = keySize.length == 4 ? ByteBuffer.wrap(keySize).getInt() : -1;
        }
        // reading key
        if (!previousPart.isKeyRead() && previousPart.isKeySizeRead()) {
            newKey = in.readNBytes(keyLength - key.length);
            previousPart = SegmentReadResult.merge(previousPart, SegmentReadResult.needMore(newKey));
        }
        // reading valueSize
        if (!previousPart.isValueSizeRead() && previousPart.isKeyRead()) {
            newValueSize = in.readNBytes(DEFAULT_LENGTH - valueSize.length);
            previousPart = SegmentReadResult.merge(previousPart, SegmentReadResult.needMore(newValueSize));
            valueSize = previousPart.getValueSizeBytes();
            valueLength = valueSize.length == 4 ? ByteBuffer.wrap(valueSize).getInt() : -1;
        }
        // reading value
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




}
