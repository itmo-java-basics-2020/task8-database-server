package ru.ifmo.database.server.logic.impl;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }
}
