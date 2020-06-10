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
            throw new DatabaseException("Table \"" + tableName + "\" is already exists");
        }
        tableDir.mkdir();

        this.currentSegment = createNewSegment();
    }

    private TableImpl(TableInitializationContext context) {
        this.tableName = context.getTableName();
        this.tableIndex = context.getTableIndex();
        this.pathToDatabaseRoot = context.getTablePath().getParent();
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
        return tableName;
    }

    private Segment createNewSegment() throws DatabaseException {
        return SegmentImpl.create(SegmentImpl.createSegmentName(tableName), Path.of(pathToDatabaseRoot.toString(), tableName));
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            if (!currentSegment.write(objectKey, objectValue)) {
                currentSegment = createNewSegment();
                if (!currentSegment.write(objectKey, objectValue)) {
                    throw new DatabaseException("Data size is more that segment size");
                }
            }
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(e.getMessage());
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);

        if (segment.isEmpty()) {
            throw new DatabaseException("Key \"" + objectKey + "\" is not exists");
        }

        try {
            return segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(e.getMessage());
        }
    }
}