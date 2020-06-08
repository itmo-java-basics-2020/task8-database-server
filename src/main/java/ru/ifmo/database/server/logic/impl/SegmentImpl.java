package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.impl.SegmentInitializationContextImpl;
import ru.ifmo.database.server.logic.Segment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private final int DEFAULT_SIZE = 100_000;

    private final String segmentName;
    private final Path segmentPath;
    private final SegmentIndex index;

    private boolean readOnly;
    private int currentSize;

    private SegmentImpl(SegmentInitializationContext context) {
        this.segmentName = context.getSegmentName();
        this.segmentPath = context.getSegmentPath();
        this.currentSize = context.getCurrentSize();
        this.index = context.getIndex();
        this.readOnly = context.getReadOnly();
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path createdSegmentPath;
        try {
            new File(tableRootPath.toString(), segmentName).createNewFile();
            createdSegmentPath = Paths.get(tableRootPath + segmentName);
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
        SegmentInitializationContext segmentContext = new SegmentInitializationContextImpl(segmentName, createdSegmentPath, 0, new SegmentIndex(), false);
        return new SegmentImpl(segmentContext);
    }

    static Segment create(String segmentName, Path tableRootPath, int segmentSizeInBytes) throws DatabaseException {
        Path createdSegmentPath;
        try {
            new File(tableRootPath.toString(), segmentName).createNewFile();
            createdSegmentPath = Paths.get(tableRootPath + segmentName);
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
        SegmentInitializationContext segmentContext = new SegmentInitializationContextImpl(segmentName, createdSegmentPath, 0, new SegmentIndex(), false, segmentSizeInBytes);
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
    public boolean write(String objectKey, String objectValue) throws IOException {
        DatabaseStoringUnit dbUnit = new DatabaseStoringUnit(objectKey, objectValue);

        if (dbUnit.getUnitSize() + currentSize > DEFAULT_SIZE) {
            readOnly = true;
            return false;
        }

        index.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(currentSize));

        DatabaseOutputStream dbOutputStream = new DatabaseOutputStream(new FileOutputStream(new File(segmentPath.toString()), true));
        currentSize += dbOutputStream.write(dbUnit);
        return true;
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<SegmentIndexInfo> offSet = index.searchForKey(objectKey);

        if (offSet.isEmpty()) {
            return null;
        }

        try {
            DatabaseInputStream dbInputStream = new DatabaseInputStream(new FileInputStream(new File(segmentPath.toString())));

            Optional<DatabaseStoringUnit> dbUnit = dbInputStream.readDbUnit(offSet.get().getOffset());

            if(dbUnit.isEmpty()) {
                throw new DatabaseException("Missed info while reading dbUnit");
            }

            return new String(dbUnit.get().getValue());
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }
}
