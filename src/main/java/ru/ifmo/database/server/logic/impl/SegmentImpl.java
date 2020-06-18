package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Segment;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

    static Segment create(String segmentName, Path tableRootPath, SegmentIndex index) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath, index, 0);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) throws DatabaseException {
        return new SegmentImpl(context);
    }

    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex index, int currentSize) throws DatabaseException {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
        this.index = index;
        this.currentSize = currentSize;

        try {
            Files.createFile(tableRootPath.resolve(segmentName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("Segment \"%s\" already exists", segmentName));
        }
    }

    private SegmentImpl(SegmentInitializationContext context) throws DatabaseException {
        this.segmentName = context.getSegmentName();
        this.tableRootPath = context.getSegmentPath();
        this.index = context.getIndex();
        this.currentSize = context.getCurrentSize();
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
            DatabaseStoringUnit databaseStoringUnit = new DatabaseStoringUnit(objectKey, objectValue);
            if (databaseStoringUnit.getValueSize() + databaseStoringUnit.getKeySize() + currentSize > FIXED_SIZE) {
                isReadOnly = true;
                return false;
            }

            SeekableByteChannel byteChannel = Files.newByteChannel(tableRootPath.resolve(segmentName), StandardOpenOption.APPEND);
            DatabaseOutputStream outputStream = new DatabaseOutputStream(Channels.newOutputStream(byteChannel));

            outputStream.write(databaseStoringUnit);

            var startPosition = byteChannel.position();
            index.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(startPosition));

        } catch (IOException e) {
            throw new DatabaseException(String.format("Segment \"%s\" does not exist", segmentName));
        }
        return true;
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<SegmentIndexInfo> indexOffset = index.searchForKey(objectKey);

        if (indexOffset.isEmpty()) {
            throw new DatabaseException("There is no such key");
        }
        try {
            SeekableByteChannel byteChannel = Files.newByteChannel(tableRootPath.resolve(segmentName), StandardOpenOption.READ);
            DatabaseInputStream in = new DatabaseInputStream(Channels.newInputStream(byteChannel));

            byteChannel.position(indexOffset.get().getOffset());

            DatabaseStoringUnit databaseStoringUnit = in.readDbUnit().orElseThrow(() -> new IllegalStateException("Not enough bytes"));
            return new String(databaseStoringUnit.getValue());
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
