package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {

    private static final int DEFAULT_CAPACITY = 100_000;

    private final String name;
    private final Path file;
    private final int capacity;
    private int fileSize;
    private boolean readOnly;
    private SegmentIndex index;

    private SegmentImpl(String name, Path tableRootPath, int capacity) throws DatabaseException {
        this.name = Objects.requireNonNull(name);
        try {
            this.file = Files.createFile(Path.of(tableRootPath.toString(), name));
        } catch (IOException ex) {
            throw new DatabaseException(
                    "Segment " + name + " cannot be created: " + ex.getMessage(),
                    ex
            );
        }
        this.capacity = capacity;
        this.fileSize = 0;
        this.readOnly = false;
        this.index = new SegmentIndex();
    }

    private SegmentImpl(String name, Path tableRootPath) throws DatabaseException {
        this(name, tableRootPath, DEFAULT_CAPACITY);
    }

    public SegmentImpl(SegmentInitializationContext context) {
        this.name = context.getSegmentName();
        this.file = context.getSegmentPath();
        this.capacity = DEFAULT_CAPACITY;
        this.fileSize = context.getCurrentSize();
        this.index = context.getIndex();
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException {
        if (this.isReadOnly()) {
            throw new DatabaseException("Segment " + this.name + " is read-only");
        }
        DatabaseStoringUnit storingUnit = new DatabaseStoringUnit(objectKey, objectValue);
        if (this.fileSize + storingUnit.length > this.capacity) {
            this.readOnly = true;
            return false;
        }
        DatabaseOutputStream output = new DatabaseOutputStream(
                Files.newOutputStream(file, StandardOpenOption.APPEND)
        );
        this.index.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(this.fileSize));
        this.fileSize += output.write(storingUnit);
        return true;
    }

    @Override
    public String read(String objectKey) throws IOException {
        Optional<SegmentIndexInfo> segmentIndexInfo = index.searchForKey(objectKey);
        long offset = segmentIndexInfo.get().getOffset();
        DatabaseInputStream input = new DatabaseInputStream(
                Files.newInputStream(file, StandardOpenOption.READ)
        );
        input.skipBytes((int) offset);
        DatabaseStoringUnit inputUnit = input.readDbUnit().get();
        return new String(inputUnit.getValue());
    }

    @Override
    public boolean isReadOnly() {
        return this.readOnly;
    }
}
