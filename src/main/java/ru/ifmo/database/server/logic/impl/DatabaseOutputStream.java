package ru.ifmo.database.server.logic.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    int write(DatabaseStoringUnit storingUnit) throws IOException {
        super.writeInt(storingUnit.getKeySize());
        super.write(storingUnit.getKey());
        super.writeInt(storingUnit.getValueSize());
        super.write(storingUnit.getValue());
        return storingUnit.length;
    }
}
