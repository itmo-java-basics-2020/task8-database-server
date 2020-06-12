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
    private final String tableName;
    private final Path pathToDatabaseRoot;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;

        File tableDir = new File(pathToDatabaseRoot.toString(), tableName);
        if (tableDir.isDirectory()) {
            throw new DatabaseException(tableName + " already exists");
        }
        if (!tableDir.mkdir()) {
            throw new DatabaseException("Can't create " + tableName);
        }

        this.currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tableDir.toPath());
    }

    private TableImpl(TableInitializationContext context) {
        this.tableName = context.getTableName();
        this.pathToDatabaseRoot = context.getTablePath().getParent();
        this.tableIndex = context.getTableIndex();
        this.currentSegment = context.getCurrentSegment();
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new TableImpl(context);
    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            if (!currentSegment.write(objectKey, objectValue)) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(this.tableName),
                        Path.of(pathToDatabaseRoot.toString(), this.tableName));
                currentSegment.write(objectKey, objectValue);
            }

            tableIndex.onIndexedEntityUpdated(objectKey, this.currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(e.getMessage());
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