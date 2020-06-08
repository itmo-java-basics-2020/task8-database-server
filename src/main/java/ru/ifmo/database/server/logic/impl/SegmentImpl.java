package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.impl.SegmentInitializationContextImpl;
import ru.ifmo.database.server.logic.Segment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private final String segmentName;
    private final Path segmentPath;
    private final SegmentIndex index;

    private int currentSize;

    private SegmentImpl(SegmentInitializationContext context) {
        this.segmentName = context.getSegmentName();
        this.segmentPath = context.getSegmentPath();
        this.currentSize = context.getCurrentSize();
        this.index = context.getIndex();
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path createdSegmentPath;
        try {
            new File(tableRootPath.toString(), segmentName).createNewFile();
            createdSegmentPath = Paths.get(tableRootPath + segmentName);
        }
        catch (Exception e) {
            throw new DatabaseException(e);
        }
        SegmentInitializationContext segmentContext = new SegmentInitializationContextImpl(segmentName, createdSegmentPath, 0, new SegmentIndex());
        return new SegmentImpl(segmentContext);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException {
        throw new UnsupportedOperationException()//todo
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
