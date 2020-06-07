package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public Optional<DatabaseStoringUnit> readDbUnit(long offset) throws IOException {
        skipNBytes(offset);

        // Read key
        int keySize = readInt();
        byte[] key = new byte[keySize];
        readNBytes(key, 0, keySize);

        // Read value
        int valueSize = readInt();
        byte[] value = new byte[valueSize];
        readNBytes(value, 0, valueSize);

        return Optional.of(new DatabaseStoringUnit(key, value));
    }
}
