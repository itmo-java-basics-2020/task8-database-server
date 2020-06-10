package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static ru.ifmo.database.server.logic.SegmentOperationResult.OperationStatus.*;

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
    private final Path pathToTableRoot;
    private final TableIndex tableIndex;

    private Segment lastSegment;

    public TableImpl(TableInitializationContext context) {
        tableName = context.getTableName();
        pathToTableRoot = context.getTablePath();
        tableIndex = context.getTableIndex();
        lastSegment = context.getCurrentSegment();
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot) throws DatabaseException {
        this.tableName = tableName;
        this.pathToTableRoot = Paths.get(pathToDatabaseRoot.toString() + File.separator + tableName);
        this.tableIndex = new TableIndex();
        File dir = new File(String.valueOf(pathToTableRoot));
        if (dir.isDirectory()) {
            throw new DatabaseException("Directory for table already exist");
        }
        if (!dir.mkdir()) {
            throw new DatabaseException("Can't create directory:" + dir.getAbsolutePath());
        }
        addSegment();
    }

    public static Table create(String tableName, Path pathToDatabaseRoot) throws DatabaseException {
        return new TableImpl(tableName, pathToDatabaseRoot);
    }

    public static Table initializeFromContext(TableInitializationContext context) throws DatabaseException {
        return new TableImpl(context);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {

        tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);
        SegmentWriteResult result = SegmentWriteResult.empty();
        while (result.getStatus() == NEED_MORE) {
            result = lastSegment.write(objectKey, objectValue, result.getNotPrintedLength());
            if (lastSegment.isReadOnly()) {
                addSegment();
            }
            if (result.getStatus() == ERROR) throw new DatabaseException(result.getErrorMessage().get());
        }
    }

    private void addSegment() {
        lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToTableRoot);
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        SegmentReadResult result = SegmentReadResult.empty();
        Optional<Segment> currentSegment = tableIndex.searchForKey(objectKey);
        if (currentSegment.isEmpty()) {
            return null;
        }
        while (result.getStatus() == NEED_MORE) {
            try {
                if (currentSegment.isPresent()) {
                    result = SegmentReadResult.merge(result, currentSegment.get().read(objectKey, result));
                    currentSegment = tableIndex.next(currentSegment.get().getName());
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new DatabaseException(e.getMessage());
            }
            if (result.getStatus() == ERROR) {
                throw new DatabaseException(result.getErrorMessage().get());
            }
        }
        //noinspection OptionalGetWithoutIsPresent
        return new String(result.getUnit().get().getValue());
    }
}