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
            in.skipNBytes(offset);
            int keyLen = in.read();
            byte[] word = new byte[keyLen];
            in.read(word, 0, keyLen);
            String key = new String(word);
            int valueLen = in.read();
            word = new byte[valueLen];
            in.read(word, 0, valueLen);
            String value = new String(word);
            return new DatabaseStoringUnit(key, value);
        } catch (Exception e) {
            return null;
        }
    }
}
