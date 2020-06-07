package ru.ifmo.database.server.logic;

import ru.ifmo.database.server.exception.DatabaseException;

public interface Table {
    String getName();

    void write(String objectKey, String objectValue) throws DatabaseException;

    String read(String objectKey) throws DatabaseException;
}
