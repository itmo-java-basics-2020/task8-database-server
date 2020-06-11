package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {
    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public DatabaseStoringUnit readDbUnit(int offset) throws IOException {
        try {
            skipNBytes(offset);

            int keySize = readInt();
            byte[] key = new byte[keySize];
            readNBytes(key, 0, keySize);

            int valueSize = readInt();
            byte[] value = new byte[valueSize];
            readNBytes(value, 0, valueSize);

            return new DatabaseStoringUnit(key, value);
        } catch (Exception e) {
            return null;
        }
    }
}
