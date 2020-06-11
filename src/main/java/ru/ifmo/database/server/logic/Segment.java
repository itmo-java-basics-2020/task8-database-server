package ru.ifmo.database.server.logic;

import ru.ifmo.database.server.exception.DatabaseException;

import java.io.IOException;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public interface Segment {

    String getName();

    boolean write(String objectKey, String objectValue) throws DatabaseException;

    String read(String objectKey) throws DatabaseException;

    boolean isReadOnly();

    void turnToReadOnly();

    Integer getSegmentSize();
}