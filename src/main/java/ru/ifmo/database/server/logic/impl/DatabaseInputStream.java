package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {
    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public Optional<DatabaseStoringUnit> readDbUnit(int offSet) throws DatabaseException {
        try {
            skipNBytes(offSet);

            int keySize = readInt();
            byte[] key = new byte[keySize];

            int valueSize = readInt();
            byte[] value = new byte[valueSize];

            try {
                for (int i = 0; i < keySize; i++) {
                    key[i] = readByte();
                }

                for (int i = 0; i < valueSize; i++) {
                    value[i] = readByte();
                }
            } catch (IOException e) {
                throw new IOException(e);
            }

            return Optional.of(new DatabaseStoringUnit(key, value));
        } catch (IOException e) {
            throw new DatabaseException("Unexpected error with InputStream");
        }
    }
}
