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
    private static final int DEFAULT_SEGMENT_SIZE = 100_000;
    private final String segmentName;
    private final Path segmentPath;
    private boolean readOnly;
    private final SegmentIndex segmentIndex;


    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.segmentPath = Path.of(tableRootPath.toString() + File.separator + segmentName);
        readOnly = false;
        segmentIndex = new SegmentIndex();
    }

    private SegmentImpl(String segmentName, Path segmentPath, boolean readOnly, SegmentIndex segmentIndex) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.readOnly = readOnly;
        this.segmentIndex = segmentIndex;
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(context.getSegmentName(),
                context.getSegmentPath(),
                context.getCurrentSize() <= DEFAULT_SEGMENT_SIZE,
                context.getIndex());
    }

    public static Segment create(String segmentName, Path tableRootPath) {
        return new SegmentImpl(segmentName, tableRootPath);
    }

    public static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }


    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public SegmentWriteResult write(String objectKey, String objectValue) {
        return write(objectKey, objectValue, -1);
    }

    @Override
    public SegmentWriteResult write(String objectKey, String objectValue, int notPrintedPart) {
        try {
            DatabaseOutputStream out = new DatabaseOutputStream(
                    new FileOutputStream(segmentPath.toString(), true), DEFAULT_SEGMENT_SIZE);
            if (notPrintedPart == -1) {
                segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(new File(segmentPath.toString()).length()));
            }
            int newNotPrintedPart = (notPrintedPart == -1) ?
                    out.write(new DatabaseStoringUnit(objectKey, objectValue)) :
                    out.write(new DatabaseStoringUnit(objectKey, objectValue), notPrintedPart);
            readOnly = newNotPrintedPart != 0;
            out.close();
            return newNotPrintedPart == 0 ? SegmentWriteResult.success() : SegmentWriteResult.needMore(newNotPrintedPart);
        } catch (IOException e) {
            return SegmentWriteResult.error(e.getMessage());
        }

    }

    @Override
    public SegmentReadResult read(String objectKey) throws IOException {
        Optional<SegmentIndexInfo> segmentIndexInfo = segmentIndex.searchForKey(objectKey);
        if (segmentIndexInfo.isEmpty()) {
            return SegmentReadResult.error("Can't find key: " + objectKey);
        }
        DatabaseInputStream in = new DatabaseInputStream(new FileInputStream(segmentPath.toString()),
                segmentIndexInfo.get().getOffset());
        SegmentReadResult result = in.readDbUnit();
        in.close();
        return result;
    }

    @Override
    public SegmentReadResult read(String objectKey, SegmentReadResult previousPart) throws IOException, DatabaseException {
        Optional<SegmentIndexInfo> offset = segmentIndex.searchForKey(objectKey);
        if (offset.isEmpty()) {
            throw new DatabaseException("ERROR WITH INDEXING");
        }
        DatabaseInputStream in = new DatabaseInputStream(new FileInputStream(segmentPath.toString()), offset.get().getOffset());
        SegmentReadResult result = in.readDbUnit(previousPart);
        in.close();
        return result;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }
}
