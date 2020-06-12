package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {
    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public Optional<DatabaseStoringUnit> readDbUnit() throws IOException {
        int keySize = super.readInt();
        byte[] key = new byte[keySize];
        super.read(key);

        int valueSize = super.readInt();
        byte[] value = new byte[valueSize];
        super.read(value);

        return Optional.of(new DatabaseStoringUnit(key, value));
    }
}
