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
    private static final long MAX_SEGMENT_SIZE = 100_000L;

    private final String segmentName;
    private final Path segmentPath;
    private final SegmentIndex segmentIndex;

    private long currentSizeInBytes;
    private volatile boolean readOnly = false;

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.segmentPath = tableRootPath.resolve(segmentName);
        this.segmentIndex = new SegmentIndex();
        this.currentSizeInBytes = 0;
    }

    public SegmentImpl(SegmentInitializationContext context) {
        this.readOnly = true;
        this.segmentName = context.getSegmentName();
        this.segmentPath = context.getSegmentPath();
        this.segmentIndex = context.getIndex();
        this.currentSizeInBytes = context.getCurrentSize();
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        SegmentImpl sg = new SegmentImpl(segmentName, tableRootPath);
        sg.initializeAsNew();
        return sg;
    }

    private void initializeAsNew() throws DatabaseException {
        //todo
    }

    @Override
    public String getName() {
        //todo
        return null;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException {
        //todo
        return false;
    }

    @Override
    public String read(String objectKey) throws IOException {
        //todo
        return null;
    }

    @Override
    public boolean isReadOnly() {
        //todo
        return false;
    }
}
