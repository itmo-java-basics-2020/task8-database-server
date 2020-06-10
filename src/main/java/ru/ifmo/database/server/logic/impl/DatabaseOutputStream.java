package ru.ifmo.database.server.logic.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    int write(DatabaseStoringUnit storingUnit) throws IOException {
        writeInt(storingUnit.getKeySize());
        for (byte b : storingUnit.getKey()) {
            writeByte(b);
        }

        writeInt(storingUnit.getValueSize());
        for (byte b : storingUnit.getValue()) {
            writeByte(b);
        }

        return storingUnit.getUnitSize();
    }
}
