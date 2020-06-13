package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
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

    private final String tablename;
    private final Path pathToDatabaseRoot;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return new CachingTable(new TableImpl(tableName, pathToDatabaseRoot, tableIndex));
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new TableImpl(context);
    }

    private TableImpl(String tablename, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tablename = tablename;
        this.tableIndex = tableIndex;
        this.pathToDatabaseRoot = pathToDatabaseRoot;

        try {
            Files.createDirectories(Path.of(pathToDatabaseRoot.toString(), tablename));
        } catch (IOException e) {
            throw new DatabaseException(String.format("Table \"%s\" already exists", tablename));
        }

        this.currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tablename), Path.of(pathToDatabaseRoot.toString(), tablename));
    }

    private TableImpl(TableInitializationContext context) {
        this.tablename = context.getTableName();
        this.tableIndex = context.getTableIndex();
        this.pathToDatabaseRoot = context.getTablePath().getParent();
        this.currentSegment = context.getCurrentSegment();
    }

    @Override
    public String getName() {
        return tablename;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            currentSegment.write(objectKey, objectValue);
            tableIndex.updateSegment(objectKey, currentSegment);
        } catch (IOException e){
            throw new DatabaseException(e);
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        try{
        if (!tableIndex.ifContains(objectKey)) {
            throw new DatabaseException("There is no such key");
        } else {
            return tableIndex.getSegment(objectKey).read(objectKey);
        }} catch (IOException e){
            throw new DatabaseException(e);
        }
    }
}