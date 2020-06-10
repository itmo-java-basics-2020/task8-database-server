package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.index.impl.SegmentIndex;
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
    public static final int DEFAUL_SIZE = 100_000;

    private final String segmentName;
    private final Path tableRootPath;
    private final SegmentIndex index;
    private int size;
    private boolean isReadOnly;

    public SegmentImpl(SegmentInitializationContext context) {
        this.segmentName = context.getSegmentName();
        this.tableRootPath = context.getSegmentPath().getParent();
        this.index = context.getIndex();
        this.size = context.getCurrentSize();
        this.isReadOnly = context.isReadOnly();
    }

    private SegmentImpl(String segmentName, Path tableRootPath) throws DatabaseException {
        this.size = 0;
        this.segmentName = segmentName;
        this.isReadOnly = false;
        this.tableRootPath = tableRootPath;
        this.index = new SegmentIndex();

        try {
            new File(tableRootPath.toString(), segmentName).createNewFile();
        } catch (IOException e) {
            throw new DatabaseException(e.getMessage());
        }
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath);
    }

    public static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return this.segmentName;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException {
        DatabaseStoringUnit unit = new DatabaseStoringUnit(objectKey, objectValue);

        if (unit.getUnitSize() + size > DEFAUL_SIZE) {
            this.isReadOnly = true;
            return false;
        }

        DatabaseOutputStream dbOutputStream = new DatabaseOutputStream(new FileOutputStream(new File(tableRootPath.toString()), true));
        this.size += dbOutputStream.write(unit);
        return true;
    }

    @Override
    public String read(String objectKey) throws IOException, DatabaseException {
        Optional<SegmentIndexInfo> offset = index.searchForKey(objectKey);

        if (offset.isEmpty()) {
            return null;
        }

        try {
            DatabaseInputStream dbInputStream = new DatabaseInputStream(new FileInputStream(new File(tableRootPath.toString())));

            Optional<DatabaseStoringUnit> dbUnit = dbInputStream.readDbUnit((int) offset.get().getOffset());

            if (dbUnit.isEmpty()) {
                throw new DatabaseException("Missed info while reading dbUnit");
            }

            return new String(dbUnit.get().getValue());
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean isReadOnly() {
        return this.isReadOnly;
    }
}
