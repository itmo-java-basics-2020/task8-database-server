package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {

    private final String name;
    private final Path directory;
    private TableIndex index;
    private Segment currentSegment;

    private TableImpl(String name, Path pathToDatabaseRoot, TableIndex index) throws DatabaseException {
        this.name = name;
        try {
            this.directory = Files.createDirectory(Path.of(pathToDatabaseRoot.toString(), name));
        } catch (IOException ex) {
            throw new DatabaseException(
                    "Table " + name + " cannot be created: " + ex.getMessage(),
                    ex
            );
        }
        this.index = index;
        this.currentSegment = SegmentImpl.create(
                SegmentImpl.createSegmentName(name),
                this.directory
        );
    }

    private TableImpl(String name, Path directory, TableIndex index, Segment currentSegment) {
        this.name = name;
        this.directory = directory;
        this.index = index;
        this.currentSegment = currentSegment;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new TableImpl(
                context.getTableName(),
                context.getTablePath(),
                context.getTableIndex(),
                context.getCurrentSegment()
        );
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        boolean success;
        try {
            // false if current segment is full
            success = this.currentSegment.write(objectKey, objectValue);
            if (!success) {
                String segmentName = SegmentImpl.createSegmentName(this.name);
                this.currentSegment = SegmentImpl.create(segmentName, this.directory);
            }
        } catch (IOException ex) {
            throw new DatabaseException(
                    "Cannot write (" + objectKey + ", " + objectValue + "): " + ex.getMessage(),
                    ex
            );
        }
        this.index.onIndexedEntityUpdated(objectKey, this.currentSegment);
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = this.index.searchForKey(objectKey);
        if (segment.isEmpty()) {
            throw new DatabaseException("No key " + objectKey + " in table " + this.name);
        }
        try {
            return segment.get().read(objectKey);
        } catch (IOException ex) {
            throw new DatabaseException(
                    "Cannot read " + objectKey + ": " + ex.getMessage(),
                    ex
            );
        }
    }
}