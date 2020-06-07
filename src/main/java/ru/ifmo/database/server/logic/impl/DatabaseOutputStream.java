package ru.ifmo.database.server.logic.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    int write(DatabaseStoringUnit storingUnit) {
        try {
            out.write(storingUnit.getKeySize());
            out.write(storingUnit.getKey());
            out.write(storingUnit.getValueSize());
            out.write(storingUnit.getValue());
            return 1;
        } catch (IOException e) {
            return 0;
        }
    }
}
