package ru.ifmo.database.server.logic.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static ru.ifmo.database.server.logic.impl.MergeArrays.*;

public class DatabaseOutputStream extends DataOutputStream {
    private static final int DEFAULT_LENGTH = 4;
    private static final int DEFAULT_SEGMENT_SIZE = 100_000;
    private int writtenLength;
    private int segmentLength;

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
        writtenLength = 0;
        segmentLength = DEFAULT_SEGMENT_SIZE;
    }

    public DatabaseOutputStream(OutputStream outputStream, int segmentLength) {
        super(outputStream);
        writtenLength = 0;
        this.segmentLength = segmentLength;
    }

    /**
     * @param storingUnit
     * @return int - how much not printed;
     * @throws IOException
     */
    public int write(DatabaseStoringUnit storingUnit) throws IOException {
        byte[] data = getConcatenatedArray(storingUnit);

        return write(data, data.length);
    }

    /**
     * Writes storage  not from the beginning of unit
     *
     * @param storingUnit
     * @param notPrintedLength
     * @return int - how much not printed
     * @throws IOException
     */
    public int write(DatabaseStoringUnit storingUnit, int notPrintedLength) throws IOException {
        byte[] data = getConcatenatedArray(storingUnit);
        return write(data, notPrintedLength);
    }

    private int write(byte[] data, int notPrintedLength) throws IOException {
        int newNotPrintedLength = Math.max(writtenLength + notPrintedLength - segmentLength, 0);

        byte[] finalData = Arrays.copyOfRange(data, data.length - notPrintedLength, data.length - newNotPrintedLength);

        writtenLength += finalData.length;
        out.write(finalData);
        return newNotPrintedLength;
    }


    private byte[] getConcatenatedArray(DatabaseStoringUnit storingUnit) {
        byte[][] arrays = {
                ByteBuffer.allocate(DEFAULT_LENGTH).putInt(storingUnit.getKeySize()).array(), // keySize
                storingUnit.getKey(), // key
                ByteBuffer.allocate(DEFAULT_LENGTH).putInt(storingUnit.getValueSize()).array(), // valueSize
                storingUnit.getValue() // value
        };
        return mergeArrays(arrays);
    }
}
