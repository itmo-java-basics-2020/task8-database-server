package ru.ifmo.database.server.logic;

import ru.ifmo.database.server.exception.DatabaseException;

import java.io.IOException;

public interface Segment {

    String getName();

    boolean write(String objectKey, String objectValue) throws IOException, DatabaseException;

    String read(String objectKey) throws IOException, DatabaseException;

    boolean isReadOnly();
}
