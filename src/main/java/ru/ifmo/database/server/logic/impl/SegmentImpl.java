package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {

    public SegmentImpl(SegmentInitializationContext context) {
        throw new UnsupportedOperationException(); // todo implement
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public String read(String objectKey) throws IOException {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public boolean isReadOnly() {
        throw new UnsupportedOperationException(); // todo implement
    }
}
