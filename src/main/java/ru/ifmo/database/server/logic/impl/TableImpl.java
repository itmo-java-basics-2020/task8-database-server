package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.io.IOException;
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
    private final TableIndex tableIndex;
    private final Path pathToDatabaseRoot;

    private Segment lastSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.name = tableName;
        this.tableIndex = tableIndex;
        this.pathToDatabaseRoot = pathToDatabaseRoot;

        File dir = new File(pathToDatabaseRoot.toString(), tableName);

        if (dir.isDirectory()) {
            throw new DatabaseException("Already exists");
        }
        dir.mkdir();

        this.lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), Path.of(pathToDatabaseRoot.toString(), tableName));
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex, int segmentSizeInBytes) throws DatabaseException {
        this.name = tableName;
        this.tableIndex = tableIndex;
        this.pathToDatabaseRoot = pathToDatabaseRoot;

        File dir = new File(pathToDatabaseRoot.toString(), tableName);

        if (dir.isDirectory()) {
            throw new DatabaseException("Already exists");
        }
        dir.mkdir();

        this.lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), Path.of(pathToDatabaseRoot.toString(), tableName), segmentSizeInBytes);
    }

    private TableImpl(TableInitializationContext context) {
        this.name = context.getTableName();
        this.tableIndex = context.getTableIndex();
        this.pathToDatabaseRoot = context.getTablePath().getParent();
        this.lastSegment = context.getCurrentSegment();
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return new CachingTable(new TableImpl(tableName, pathToDatabaseRoot, tableIndex));
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex, int segmentSizeInBytes) throws DatabaseException {
        return new CachingTable(new TableImpl(tableName, pathToDatabaseRoot, tableIndex, segmentSizeInBytes));
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new TableImpl(context);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            if (!lastSegment.write(objectKey, objectValue)) {
                lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), Path.of(pathToDatabaseRoot.toString(), name));
                lastSegment.write(objectKey, objectValue);
            }

            tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<Segment> objectSegment = tableIndex.searchForKey(objectKey);

        if (objectSegment.isEmpty()) {
            return null;
        }

        try {
            return objectSegment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }
}