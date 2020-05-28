package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.InputStream;

public class DatabaseInputStream extends DataInputStream {
    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }
}
