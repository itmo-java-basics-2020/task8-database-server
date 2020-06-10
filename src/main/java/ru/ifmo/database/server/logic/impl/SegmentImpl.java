package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.impl.SegmentInitializationContextImpl;
import ru.ifmo.database.server.logic.Segment;

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
    private final int FIXED_SIZE = 100_000;

    private final String segmentName;
    private final Path tableRootPath;
    private final SegmentIndex index;

    private boolean isReadOnly;
    private int currentSize;

    public SegmentImpl(SegmentInitializationContext context) {
        this.segmentName = context.getSegmentName();
        this.tableRootPath = context.getSegmentPath();
        this.index = context.getIndex();
        this.currentSize = context.getCurrentSize();
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        try {
            Files.createFile(Path.of(tableRootPath.toString(), segmentName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("Segment \"%s\" already exists", segmentName));
        }
        SegmentInitializationContext context = new SegmentInitializationContextImpl(segmentName, Paths.get(tableRootPath + segmentName), 0, new SegmentIndex());
        return new SegmentImpl(context);
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
        try {
            int indexOffset = (int) Files.size(Files.createFile(Paths.get(tableRootPath + segmentName)));
            DatabaseStoringUnit databaseStoringUnit = new DatabaseStoringUnit(objectKey, objectValue);
            if (databaseStoringUnit.getValueSize() + databaseStoringUnit.getKeySize() + currentSize > FIXED_SIZE) {
                isReadOnly = true;
                return false;
            }
            return (new DatabaseOutputStream((new FileOutputStream(Paths.get(tableRootPath + segmentName).toString()))).write(databaseStoringUnit)) == 1;
        } catch (IOException e) {
            throw new DatabaseException(String.format("Segment \"%s\" does not exist", segmentName));
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        int indexOffset = (int) index.getSegmentIndexInfo(objectKey).getOffset();
        try {
            DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(Paths.get(tableRootPath + segmentName).toString()));
            Optional<DatabaseStoringUnit> databaseStoringUnit = databaseInputStream.readDbUnit(indexOffset);
            return new String(databaseStoringUnit.get().getValue());
        } catch (DatabaseException e) {
            throw new DatabaseException("DbUnit reading error");
        } catch (IOException e1) {
            throw new DatabaseException("There is no value connected to the key");
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }
}
