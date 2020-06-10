package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private static final Integer DEFAULT_SEGMENT_SIZE = 1000;

    private final String segmentName;
    private final Path tableRootPath;
    private final SegmentIndex segmentIndex;
    private boolean isReadOnly;
    private Integer currentSize;

    public SegmentImpl(SegmentInitializationContext context) {
        segmentName = context.getSegmentName();
        currentSize = context.getCurrentSize();
        tableRootPath = context.getSegmentPath().getParent();
        isReadOnly = context.isSegmentReadOnly();
        segmentIndex = context.getIndex();
    }

    private SegmentImpl(String segmentName, Path tableRootPath) throws DatabaseException {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
        this.segmentIndex = new SegmentIndex();
        this.isReadOnly = false;
        this.currentSize = 0;

        try {
            new File(tableRootPath.toString(), segmentName).createNewFile();
        } catch (IOException e) {
            throw new DatabaseException(e.getMessage());
        }
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath);
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
        DatabaseStoringUnit data = new DatabaseStoringUnit(objectKey, objectValue);

        if (data.getFileSize() + currentSize > DEFAULT_SEGMENT_SIZE) {
            isReadOnly = true;
            return false;
        }

        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(currentSize));
        DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(new File(tableRootPath.toString(), segmentName), true));
        currentSize += outputStream.write(data);
        return true;
    }

    @Override
    public String read(String objectKey) throws IOException, DatabaseException {
        Optional<SegmentIndexInfo> offset = segmentIndex.searchForKey(objectKey);

        if (offset.isEmpty()) {
            throw new DatabaseException("SegmentIndex does not have key \"" + objectKey + "\"");
        }

        DatabaseInputStream inputStream = new DatabaseInputStream(
                new FileInputStream(
                        new File(tableRootPath.toString(), segmentName)));
        try (DatabaseInputStream in = inputStream) {
            Optional<DatabaseStoringUnit> data = in.readDbUnit(offset.get().getOffset());

            if (data.isEmpty()) {
                throw new DatabaseException("File is empty");
            }

            return new String(data.get().getValue());
        } catch (IOException e) {
            throw new DatabaseException("File changed");
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }
}
